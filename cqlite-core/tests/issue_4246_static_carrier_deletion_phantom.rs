//! Issue #4246 — a STATIC-ROW CARRIER's own row deletion must not reach
//! `Statistics.db`, because no production path ever writes it to `Data.db`.
//!
//! ## The adjudication this file pins
//!
//! Roborev rounds 6/7 on PR #4289 added a group-level row-deletion fold to
//! `stats_fold::fold_static_carrier_stats`, reasoning that a static carrier's
//! `CellOperation::DeleteRow` / #932 `Mutation::row_tombstone` was otherwise
//! "folded nowhere". Round 8 overturned that: the fold was a PHANTOM MARKER —
//! a persisted stats entry (minima plus an `estimatedTombstoneDropTime`
//! histogram increment) for a deletion with NO corresponding bytes in
//! `Data.db`. That is the exact "persisted stats do not match emitted data"
//! defect class issue #4246 exists to eliminate, so the fold was an instance
//! of the bug rather than a fix for one, and it was removed.
//!
//! ## Format authority (pinned `cassandra-5.0.8`, never CQLite's own code)
//!
//! Cassandra collects a static row's statistics from THE SAME `Row` object it
//! has just serialized, in adjacent statements —
//! `src/java/org/apache/cassandra/io/sstable/format/SortedTableWriter.java`,
//! `addStaticRow`:
//!
//! ```java
//! partitionWriter.addStaticRow(row);
//! if (!row.isEmpty())
//!     Rows.collectStats(row, metadataCollector);
//! ```
//!
//! and `db/rows/Rows.java::collectStats` reads `row.deletion().time()` — the
//! very field `db/rows/UnfilteredSerializer.java::serialize` turns into the
//! `HAS_DELETION` (`0x10`) row flag. Emission and collection therefore cannot
//! diverge by construction: Cassandra counts a static-row deletion IF AND ONLY
//! IF it wrote one.
//!
//! ## Why CQLite can never write one (the emitter trace)
//!
//! * `data_writer/encoding.rs::is_static_operation` returns `false` for
//!   `CellOperation::DeleteRow`, and `data_writer/static_ops.rs`'s
//!   `StaticOpsTracker::feed` additionally `continue`s on it — so
//!   `collect_static_operations` can never place one in the merged set.
//! * `data_writer/static_rows.rs::write_static_row_with_prev_size` sets
//!   `ROW_HAS_DELETION` only if its `static_ops` slice holds a `DeleteRow`,
//!   and never consults `Mutation::row_tombstone` at all.
//! * Every production static-row emission passes a merged set into that
//!   function. `DataWriter::write_static_row` — the one entry point that maps
//!   `mutation.operations` unfiltered — has no production caller.
//!
//! ## Why #1721 does not apply
//!
//! Rounds 6/7 cited issue #1721's below-baseline guard. Commit `e638bf369`'s
//! regression builds `clustering_key: Some(ck)` against a schema whose columns
//! are ALL `is_static: false` — a CLUSTERING row in a table with no static
//! columns, which `is_static_row_mutation` rejects twice over. #1721's deletion
//! IS emitted and IS folded, at the group level, on the clustering-row path.
//!
//! ## What each test asserts
//!
//! The two halves are pinned in BOTH directions so neither can drift alone:
//! the emitted flags byte (no `ROW_HAS_DELETION`) AND the persisted histogram
//! (no bucket). The byte assertions are the tripwire: if the emitter is ever
//! taught to write a static-row deletion, they FAIL, and whoever does that
//! must revisit `fold_static_carrier_stats`. The final test is the
//! live-control proving the histogram assertion is not vacuously green.

#![cfg(feature = "write-support")]

use std::path::{Path, PathBuf};

use cqlite_core::parser::enhanced_statistics_parser::parse_statistics_with_fallback;
use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::mutation::{
    CellOperation, ClusteringKey, Mutation, PartitionKey, TableId,
};
use cqlite_core::storage::write_engine::{WriteEngine, WriteEngineConfig};
use cqlite_core::types::Value;
use tempfile::TempDir;

const KS: &str = "issue_4246_static_ks";
const TBL: &str = "static_carrier";

/// `UnfilteredSerializer.HAS_DELETION`, cassandra-5.0.8
/// (`db/rows/UnfilteredSerializer.java`: `HAS_DELETION = 0x10`).
const ROW_HAS_DELETION: u8 = 0x10;
/// `UnfilteredSerializer.EXTENSION_FLAG` (`0x80`) — always set on a static row.
const ROW_HAS_EXTENDED_FLAGS: u8 = 0x80;
/// `UnfilteredSerializer.IS_STATIC` extended flag (`0x01`).
const EXTENDED_IS_STATIC: u8 = 0x01;

/// The pinned local deletion time every deletion in this file is stamped with
/// — far enough in the future that gc-grace never purges the marker, and
/// distinctive enough to identify unambiguously in the drop-time histogram.
const PINNED_LDT: i32 = 2_000_000_000;

fn col(name: &str, ty: &str, is_static: bool) -> Column {
    Column {
        name: name.to_string(),
        data_type: ty.to_string(),
        nullable: true,
        default: None,
        is_static,
    }
}

/// A STATIC-BEARING schema: `stat_col` is the static column, so
/// `is_static_row_mutation` classifies a `clustering_key: None` mutation as a
/// static-row carrier (this is the classification under test).
fn schema() -> TableSchema {
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
            order: ClusteringOrder::Asc,
        }],
        columns: vec![
            col("id", "int", false),
            col("ck", "int", false),
            col("stat_col", "text", true),
            col("name", "text", false),
        ],
        comments: Default::default(),
        dropped_columns: Default::default(),
    }
}

/// A live clustering row, so every partition under test also contains a real
/// row (the static prelude is never the only thing in the partition).
fn live_row(ck: i32, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("id", Value::Integer(1)),
        Some(ClusteringKey::single("ck", Value::Integer(ck))),
        vec![CellOperation::Write {
            column: "name".to_string(),
            value: Value::text("v"),
        }],
        ts,
        None,
    )
}

/// REPRESENTATION 1 — a static carrier whose only operation is `DeleteRow`.
/// `is_static_row_mutation` accepts it (`DeleteRow` maps to `true` in its
/// `all(..)` predicate), so it is routed to the static-carrier branch.
fn static_carrier_delete_row_op(ts: i64) -> Mutation {
    Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![CellOperation::DeleteRow],
        ts,
        None,
    )
    .with_local_deletion_time(PINNED_LDT)
}

/// REPRESENTATION 2 — a static carrier with a live static cell plus a #932
/// DECOUPLED `Mutation::row_tombstone`, whose deletion time is independent of
/// `timestamp_micros`. This is the representation issue #1721 fixed on the
/// CLUSTERING-row path; here it rides a static carrier.
fn static_carrier_row_tombstone_field(ts: i64, del_ts: i64) -> Mutation {
    Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![CellOperation::Write {
            column: "stat_col".to_string(),
            value: Value::text("s"),
        }],
        ts,
        None,
    )
    .with_row_tombstone(del_ts, PINNED_LDT)
}

/// The CONTROL shape: the SAME deletion on a CLUSTERING row, which the writer
/// really does emit (`merge_row_group` → `RowWrite.row_deletion` →
/// `ROW_HAS_DELETION`) and must therefore really count.
fn clustering_row_delete_row_op(ck: i32, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("id", Value::Integer(1)),
        Some(ClusteringKey::single("ck", Value::Integer(ck))),
        vec![CellOperation::DeleteRow],
        ts,
        None,
    )
    .with_local_deletion_time(PINNED_LDT)
}

fn find_exactly_one(dir: &Path, suffix: &str) -> PathBuf {
    fn collect(dir: &Path, suffix: &str, out: &mut Vec<PathBuf>, depth: usize) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path
                .file_name()
                .map(|n| n.to_string_lossy().ends_with(suffix))
                .unwrap_or(false)
            {
                out.push(path);
            } else if depth > 0 && path.is_dir() {
                collect(&path, suffix, out, depth - 1);
            }
        }
    }
    let mut found = Vec::new();
    collect(dir, suffix, &mut found, 8);
    assert_eq!(
        found.len(),
        1,
        "expected exactly one *{suffix} under {dir:?}, found {found:?}"
    );
    found.pop().expect("exactly one match, just asserted")
}

/// Flush one batch through the real public `WriteEngine` (the buffered
/// `SSTableWriter::write_partition` path, including the issue #729 two-pass
/// baseline pre-scan) into a fresh directory, and return it.
fn flush_batch(temp: &TempDir, name: &str, mutations: Vec<Mutation>) -> PathBuf {
    let data_dir = temp.path().join(name);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let mut engine = WriteEngine::new(WriteEngineConfig::new(
        data_dir.clone(),
        temp.path().join(format!("{name}-wal")),
        schema(),
    ))
    .expect("WriteEngine::new");
    for mutation in mutations {
        engine.write(mutation).expect("write");
    }
    rt.block_on(engine.flush())
        .expect("flush")
        .expect("flush produced an SSTable");
    data_dir
}

/// The `(row_flags, extended_flags)` byte pair of the partition's FIRST
/// unfiltered, which for a static-bearing partition is the static row.
///
/// The prefix is fully determined by the `nb`/BIG partition header layout
/// (`docs/sstables-definitive-guide/chapters/05-data-db-format.md`):
/// `[key_len: u16][key][partition localDeletionTime: i32][markedForDeleteAt:
/// i64]`, then the first unfiltered's flags byte. Each step is asserted rather
/// than assumed, so a layout change fails loudly here instead of silently
/// reading the wrong byte.
fn static_row_flag_bytes(data_db: &Path) -> (u8, u8) {
    let bytes = std::fs::read(data_db).expect("read Data.db");
    assert!(bytes.len() > 16, "Data.db is implausibly short: {bytes:?}");
    let key_len = u16::from_be_bytes([bytes[0], bytes[1]]) as usize;
    assert_eq!(key_len, 4, "single `int` partition key is 4 bytes");
    let after_key = 2 + key_len;
    assert_eq!(
        &bytes[after_key..after_key + 4],
        &0x7fff_ffffu32.to_be_bytes(),
        "no partition tombstone in this fixture, so the partition header's \
         localDeletionTime must be the LIVE sentinel"
    );
    let flags_at = after_key + 4 + 8;
    let flags = bytes[flags_at];
    assert_eq!(
        flags & ROW_HAS_EXTENDED_FLAGS,
        ROW_HAS_EXTENDED_FLAGS,
        "the first unfiltered of a static-bearing partition is the static row, \
         which always carries EXTENSION_FLAG (flags byte {flags:#04x})"
    );
    let extended = bytes[flags_at + 1];
    assert_eq!(
        extended & EXTENDED_IS_STATIC,
        EXTENDED_IS_STATIC,
        "the first unfiltered must be IS_STATIC (extended flags {extended:#04x})"
    );
    (flags, extended)
}

/// Every `(local_deletion_time, count)` bucket of the persisted
/// `estimatedTombstoneDropTime` histogram — the counter
/// `StatisticsMetadata::update_local_deletion_time` increments once per folded
/// tombstone (it is NOT idempotent, so a phantom fold shows up here even when
/// it happens to agree with an existing minimum).
fn tombstone_drop_buckets(data_dir: &Path) -> Vec<(i64, u64)> {
    let path = find_exactly_one(data_dir, "-Statistics.db");
    let bytes = std::fs::read(&path).expect("read Statistics.db");
    let (_, stats) = parse_statistics_with_fallback(&bytes, None).expect("decode Statistics.db");
    stats.tombstone_drop_times.clone()
}

#[test]
fn static_carrier_delete_row_op_emits_no_static_row_deletion() {
    let temp = TempDir::new().unwrap();
    let dir = flush_batch(
        &temp,
        "delete-row-op",
        vec![
            live_row(1, 1_000_000),
            static_carrier_delete_row_op(900_000),
        ],
    );
    let data_db = find_exactly_one(&dir, "-big-Data.db");
    let (flags, _) = static_row_flag_bytes(&data_db);
    assert_eq!(
        flags & ROW_HAS_DELETION,
        0,
        "a static carrier's `CellOperation::DeleteRow` is dropped by \
         `collect_static_operations`, so the emitted static row must NOT set \
         ROW_HAS_DELETION (got flags {flags:#04x}). If this now FAILS because \
         the emitter legitimately learned to write static-row deletions, \
         `stats_fold::fold_static_carrier_stats` must fold the marker again — \
         see its doc comment."
    );
}

#[test]
fn static_carrier_row_tombstone_field_emits_no_static_row_deletion() {
    let temp = TempDir::new().unwrap();
    let dir = flush_batch(
        &temp,
        "row-tombstone-field",
        vec![
            live_row(1, 1_000_000),
            static_carrier_row_tombstone_field(1_000_000, 900_000),
        ],
    );
    let data_db = find_exactly_one(&dir, "-big-Data.db");
    let (flags, _) = static_row_flag_bytes(&data_db);
    assert_eq!(
        flags & ROW_HAS_DELETION,
        0,
        "`write_static_row_with_prev_size` never consults \
         `Mutation::row_tombstone`, so a static carrier's #932 decoupled row \
         tombstone must NOT set ROW_HAS_DELETION (got flags {flags:#04x})"
    );
}

#[test]
fn static_carrier_deletion_adds_no_tombstone_drop_time_bucket() {
    for (name, carrier) in [
        ("delete-row-op-stats", static_carrier_delete_row_op(900_000)),
        (
            "row-tombstone-field-stats",
            static_carrier_row_tombstone_field(1_000_000, 900_000),
        ),
    ] {
        let temp = TempDir::new().unwrap();
        let dir = flush_batch(&temp, name, vec![live_row(1, 1_000_000), carrier]);
        let buckets = tombstone_drop_buckets(&dir);
        assert!(
            buckets.is_empty(),
            "{name}: a static carrier's row deletion is never emitted to \
             Data.db, so it must contribute 0 RECOGNISED tombstone-drop-time \
             observations to Statistics.db — got {buckets:?}. A bucket at \
             {PINNED_LDT} here is the roborev round-6/7 PHANTOM fold \
             (issue #4246): a stats entry for bytes that were never written."
        );
    }
}

#[test]
fn clustering_row_deletion_still_adds_a_tombstone_drop_time_bucket() {
    // LIVE CONTROL for the assertion above: the SAME deletion, moved onto a
    // CLUSTERING row, IS emitted (`merge_row_group` → ROW_HAS_DELETION) and so
    // MUST still be counted. Without this, an empty-histogram assertion would
    // pass just as well if the writer had stopped folding tombstones entirely.
    let temp = TempDir::new().unwrap();
    let dir = flush_batch(
        &temp,
        "clustering-control",
        vec![
            live_row(1, 1_000_000),
            clustering_row_delete_row_op(2, 900_000),
        ],
    );
    let buckets = tombstone_drop_buckets(&dir);
    assert_eq!(
        buckets,
        vec![(PINNED_LDT as i64, 1)],
        "a CLUSTERING row's `DeleteRow` IS emitted with ROW_HAS_DELETION, so it \
         must contribute EXACTLY ONE tombstone-drop-time observation at its \
         pinned LDT — proving the empty-histogram assertion in \
         `static_carrier_deletion_adds_no_tombstone_drop_time_bucket` is a live \
         signal, not a vacuous one"
    );
}
