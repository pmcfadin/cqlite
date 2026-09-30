//! Issue #4246 — a STATIC-ROW CARRIER's own row deletion must not reach
//! `Statistics.db`, because no production path ever writes it to `Data.db`.
//!
//! ## TWO independent paths had to exclude it (rounds 8 and 10)
//!
//! A mutation reaches the persisted `Statistics.db` minima by two separate
//! routes, each of which re-derives "was this actually emitted?" by hand:
//!
//! 1. the DIRECT fold — `stats_fold::fold_static_carrier_stats`, called by
//!    `SSTableWriter::write_partition`, `KWayMerger::merge` and
//!    `WriteEngine::maintenance_step` (the phantom was removed here in
//!    ROUND 8; pinned by the first four tests below); and
//! 2. the TWO-PASS PRE-SEED ENCODING BASELINE —
//!    `SSTableWriter::compute_mutations_baseline_stats` ->
//!    `fold_one_mutation_baseline` -> `pre_seed_encoding_baselines` (issue
//!    #729), removed in ROUND 10 and pinned by the last two tests.
//!
//! Round 8's fix was correct but NOT sufficient, and its comments overstated
//! it as an absolute ("folded nowhere"): the pre-seed path assigns its
//! `min_local_deletion_time` into the persisted field VERBATIM, and
//! `write_partition`'s later fold is a `.min()` that can only lower it
//! further — so the phantom survived rounds 8 and 9 untouched. It was
//! invisible to the histogram assertions here because pre-seeding is a field
//! assignment, not an `update_local_deletion_time` call. Issue **#4320**
//! tracks collapsing the two re-derivations into one; until then a change to
//! either path must be mirrored in the other.
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
//! The halves are pinned in BOTH directions so none can drift alone: the
//! emitted flags byte (no `ROW_HAS_DELETION`), the persisted histogram (no
//! bucket, path 1), and the persisted `minLocalDeletionTime` read back off
//! disk (path 2). The byte assertions are the tripwire: if the emitter is
//! ever taught to write a static-row deletion, they FAIL, and whoever does
//! that must revisit BOTH `fold_static_carrier_stats` and
//! `fold_one_mutation_baseline`. Each persisted-value assertion is paired
//! with a live control proving it is not vacuously green.

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

// ---------------------------------------------------------------------------
// The SECOND mechanism: the two-pass pre-seed ENCODING BASELINE (issue #4246
// roborev round 10).
//
// Round 8 (above) closed the DIRECT fold path — `fold_static_carrier_stats`,
// which `write_partition`/`KWayMerger::merge`/`maintenance_step` call to fold a
// carrier into `self.stats`. It did NOT close the SECOND, independent path by
// which the same phantom deletion reaches the same persisted `Statistics.db`:
//
//   `WriteEngine::flush_internal_async`
//     -> `SSTableWriter::compute_mutations_baseline_stats`   (#729 pre-scan)
//          -> `fold_one_mutation_baseline`  <-- folded `DeleteRow`'s LDT here
//     -> `SSTableWriter::pre_seed_encoding_baselines`
//          -> `self.stats.min_local_deletion_time = <that value>`  (VERBATIM)
//     -> `write_partition` ... -> `Statistics.db`
//
// `pre_seed_encoding_baselines` ASSIGNS the pre-scan's `min_ldt` into the very
// field that is persisted, and `write_partition`'s own fold runs strictly after
// and can only LOWER it further (`update_local_deletion_time` is a `.min()`),
// never raise it back. So a phantom contribution made in the pre-scan survives
// into the persisted minimum no matter what the direct fold path does.
//
// Crucially this is INVISIBLE to the histogram assertions above:
// `pre_seed_encoding_baselines` is a plain field assignment, NOT a call to
// `StatisticsMetadata::update_local_deletion_time`, so it never increments an
// `estimatedTombstoneDropTime` bucket. That is exactly how the round-8 fix
// escaped detection for two more rounds — the tests asserted the histogram and
// the in-memory struct, never the persisted MINIMUM parsed back off disk.
//
// The tests below therefore assert the value CQLite actually wrote into
// `Statistics.db`, read back with the same parser the reader uses.
// ---------------------------------------------------------------------------

/// The LDT of a deletion that IS genuinely emitted to `Data.db` in the fixtures
/// below. Chosen strictly ABOVE [`PINNED_LDT`] so the persisted minimum
/// discriminates: `PINNED_LDT` persisted == the phantom won, `EMITTED_LDT`
/// persisted == only emitted deletions were folded. Using two distinct values
/// (rather than a lone carrier and a sentinel) keeps the assertion a positive
/// one, immune to any future change in how the writer normalises "no deletions".
const EMITTED_LDT: i32 = 2_100_000_000;

/// A CLUSTERING row carrying a single cell tombstone with an explicit, higher
/// LDT. The writer really does emit this (a `Delete` cell inside a normal row),
/// so its LDT legitimately belongs in the persisted minimum.
fn clustering_cell_delete(ck: i32, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("id", Value::Integer(1)),
        Some(ClusteringKey::single("ck", Value::Integer(ck))),
        vec![CellOperation::Delete {
            column: "name".to_string(),
            local_deletion_time: Some(EMITTED_LDT),
        }],
        ts,
        None,
    )
}

/// A STATIC carrier whose only operation deletes a STATIC COLUMN. Unlike
/// `DeleteRow`, `is_static_operation` returns TRUE for this, so
/// `collect_static_operations` keeps it and the emitter really does write a
/// static cell tombstone — its LDT MUST still reach the persisted minimum.
/// This is the asymmetry the fix has to preserve.
fn static_carrier_static_cell_delete(ts: i64) -> Mutation {
    Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![CellOperation::Delete {
            column: "stat_col".to_string(),
            local_deletion_time: Some(PINNED_LDT),
        }],
        ts,
        None,
    )
}

/// The persisted `minLocalDeletionTime` CQLite wrote, read back off disk with
/// the reader's own parser.
///
/// SCOPE, stated exactly. `timestamp_stats.min_deletion_time` is decoded from
/// the `Statistics.db` SERIALIZATION_HEADER `EncodingStats` VInt triple
/// (`enhanced_statistics_parser::encoding_stats`), which is one of the TWO
/// on-disk fields `StatisticsMetadata::min_local_deletion_time` feeds —
/// `stats_writer/serialization_header.rs` writes this one and
/// `stats_writer/components.rs` writes the STATS component's own
/// `minLocalDeletionTime` from the SAME struct field, so a phantom in that
/// field lands in both. The parser exposes only the header value (the STATS
/// post-pass deliberately leaves `min_deletion_time` alone and recovers only
/// `max_deletion_time`), so that is the one asserted here; it is the value
/// `pre_seed_encoding_baselines` assigns into, which is what round 10 found
/// contaminated.
fn persisted_min_local_deletion_time(data_dir: &Path) -> i64 {
    let path = find_exactly_one(data_dir, "-Statistics.db");
    let bytes = std::fs::read(&path).expect("read Statistics.db");
    let (_, stats) = parse_statistics_with_fallback(&bytes, None).expect("decode Statistics.db");
    stats.timestamp_stats.min_deletion_time
}

#[test]
fn static_carrier_delete_row_op_does_not_lower_persisted_min_local_deletion_time() {
    for (name, carrier) in [
        (
            "preseed-delete-row-op",
            static_carrier_delete_row_op(900_000),
        ),
        (
            "preseed-row-tombstone-field",
            static_carrier_row_tombstone_field(1_000_000, 900_000),
        ),
    ] {
        let temp = TempDir::new().unwrap();
        let dir = flush_batch(
            &temp,
            name,
            vec![
                live_row(1, 1_000_000),
                clustering_cell_delete(2, 950_000),
                carrier,
            ],
        );
        assert_eq!(
            persisted_min_local_deletion_time(&dir),
            EMITTED_LDT as i64,
            "{name}: the ONLY deletion this batch emits to Data.db is the \
             clustering-row cell tombstone at {EMITTED_LDT}; the static \
             carrier's own row deletion at {PINNED_LDT} is dropped by \
             `collect_static_operations` and never written. Persisting \
             {PINNED_LDT} means the #729 two-pass pre-scan \
             (`compute_mutations_baseline_stats` -> \
             `fold_one_mutation_baseline`) folded a PHANTOM LDT into \
             `pre_seed_encoding_baselines`, which assigns it VERBATIM into the \
             persisted `min_local_deletion_time` (issue #4246 roborev round 10)."
        );
    }
}

#[test]
fn static_carrier_static_cell_delete_does_lower_persisted_min_local_deletion_time() {
    // LIVE CONTROL for the two assertions above, pinning the ASYMMETRY: a
    // `Delete` targeting a STATIC COLUMN on the very same carrier shape IS
    // emitted, so its (lower) LDT MUST still win the persisted minimum. Without
    // this, the fix could "pass" by gating the whole static-carrier branch out
    // of the baseline, silently under-seeding a real emitted tombstone and
    // re-opening the below-baseline delta underflow the pre-scan exists to
    // prevent.
    let temp = TempDir::new().unwrap();
    let dir = flush_batch(
        &temp,
        "preseed-static-cell-delete",
        vec![
            live_row(1, 1_000_000),
            clustering_cell_delete(2, 950_000),
            static_carrier_static_cell_delete(900_000),
        ],
    );
    assert_eq!(
        persisted_min_local_deletion_time(&dir),
        PINNED_LDT as i64,
        "a static carrier's `Delete {{ column: \"stat_col\" }}` IS emitted \
         (`is_static_operation` returns true for it, so \
         `collect_static_operations` keeps it), so its LDT at {PINNED_LDT} must \
         still win the persisted minimum over the clustering deletion at \
         {EMITTED_LDT} — only `DeleteRow` is the phantom"
    );
}
