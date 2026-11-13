//! Issue #4193, requirement R2 — the verdict vocabulary is closed and each
//! verdict has a Cassandra-written fixture where it is the only correct
//! answer (spec `reconcile-decision-trail` R2.2, R2.3).
//!
//! Primary fixture: `test_explain/trace_decisions-c2f35e90b57011f183ec5947a78bc662`
//! (design.md §D6), a purpose-built 2-generation table covering 7 of the 8
//! verdicts in ONE table; `test_tomb/dropped_regular_col` supplies the 8th
//! (`dropped-column`). Every expected literal below is read from
//! `test-data/datasets/sstables/test_explain/README.md` (whose own values I
//! independently re-derived from the fixture's `*-Data.db.jsonl` sstabledump
//! output in this session — see the per-test comments) or, for the
//! dropped-column case, from the schema's own authoritative
//! `dropped_columns` map (never a guessed literal), per the no-heuristics
//! mandate. Format authority for every rule: `cassandra-5.0.8`
//! `db/rows/Cells.java::reconcile`/`resolveRegular` (timestamp + equal-ts
//! tombstone precedence), `db/rows/Rows.java::merge` +
//! `db/rows/BTreeRow.java` (row/complex deletion), `db/DeletionTime.java`
//! (deletion coverage), `db/rows/AbstractCell.java::isLive,purge` (expiry),
//! `db/partitions/PurgeFunction.java` (strict `localDeletionTime < gcBefore`).

#![cfg(feature = "write-support")]

use std::path::{Path, PathBuf};

use cqlite_core::schema::cql_parser::{
    classify_statement, parse_create_table, split_cql_statements, StatementType,
};
use cqlite_core::schema::TableSchema;
use cqlite_core::storage::partition_key_codec::encode_partition_key_columns;
use cqlite_core::storage::scan_cancel::ScanCancel;
use cqlite_core::storage::write_engine::merge::trace::{
    CellDecision, DecidedBy, ProbeOutcome, RecordingSink, TombstoneKind, TombstoneRecord, Verdict,
};
use cqlite_core::storage::write_engine::merge::{
    build_single_partition_merger_with_trace, effective_compaction_schema, MergeStep,
};
use cqlite_core::storage::write_engine::ClusteringKey;
use cqlite_core::types::Value;

#[path = "support/datasets_root.rs"]
mod datasets_root;

// ===========================================================================
// Schema + generation resolution
// ===========================================================================

/// Every fixture's `local_deletion_time` for a tombstone written by generation
/// B (`nb-2-big`), re-derived from `nb-2-big-Data.db.jsonl`
/// (`"local_delete_time": "2026-09-21T03:58:55Z"`) — the README states this
/// value once ("B's tombstones have local deletion time 1789963135") and this
/// re-derivation confirms it independently for every case that cites it.
const GEN_B_LDT: i32 = 1_789_963_135;
/// `gc_grace_seconds` declared on `test_explain.trace_decisions`
/// (`test-data/schemas/explain-trace.cql`).
const GC_GRACE_SECONDS: i64 = 864_000;
/// The pinned `now` for every "ordinary" (non-TTL, non-boundary) case
/// (README: "Evaluate ordinary cases at now=1789963136").
const NOW_ORDINARY: i64 = 1_789_963_136;

/// Selects the ONE `CREATE TABLE` statement for `table` out of a
/// multi-statement `.cql` fixture file, setting `keyspace` explicitly since
/// `parse_create_table` has no cross-statement `USE <keyspace>;` context.
fn load_table_schema(cql_path: &Path, keyspace: &str, table: &str) -> TableSchema {
    let content = std::fs::read_to_string(cql_path)
        .unwrap_or_else(|error| panic!("read {}: {error}", cql_path.display()));
    for statement in split_cql_statements(&content) {
        if !matches!(classify_statement(&statement), StatementType::CreateTable) {
            continue;
        }
        let (_, mut schema) = parse_create_table(&statement).unwrap_or_else(|error| {
            panic!("parse CREATE TABLE in {}: {error:?}", cql_path.display())
        });
        if schema.table.eq_ignore_ascii_case(table) {
            schema.keyspace = keyspace.to_string();
            return schema;
        }
    }
    panic!(
        "table {keyspace}.{table} not found in {}",
        cql_path.display()
    );
}

/// Newest-to-oldest `nb-N-big-Data.db` paths directly under `dir` (both
/// `trace_decisions` generations live flat in one directory, never nested) —
/// `run_index` 0 must be the NEWEST generation (`nb-2-big`), matching every
/// other production caller's "run index = LWW tie-break rank" convention
/// (`point_read.rs`).
fn discover_generations_newest_first(dir: &Path) -> Vec<PathBuf> {
    let mut found: Vec<(u64, PathBuf)> = std::fs::read_dir(dir)
        .unwrap_or_else(|error| panic!("read_dir {}: {error}", dir.display()))
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            let path = entry.path();
            let name = path.file_name()?.to_str()?.to_string();
            if name.starts_with("nb-") && name.ends_with("-big-Data.db") {
                let generation = name
                    .strip_prefix("nb-")?
                    .split("-big-")
                    .next()?
                    .parse::<u64>()
                    .ok()?;
                Some((generation, path))
            } else {
                None
            }
        })
        .collect();
    found.sort_by_key(|(generation, _)| std::cmp::Reverse(*generation));
    found.into_iter().map(|(_, path)| path).collect()
}

fn trace_decisions_generation_dir() -> PathBuf {
    let root = datasets_root::sstables_root_for_table("test_explain", "trace_decisions")
        .unwrap_or_else(|| {
            panic!(
                "{}",
                datasets_root::describe_search("test_explain", "trace_decisions")
            )
        });
    datasets_root::table_generation_dirs(&root, "test_explain", "trace_decisions")
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("no usable test_explain.trace_decisions generation dir"))
}

fn trace_decisions_schema() -> TableSchema {
    let schema_path = datasets_root::schema_path("explain-trace.cql")
        .expect("committed test-data/schemas/explain-trace.cql fixture");
    load_table_schema(&schema_path, "test_explain", "trace_decisions")
}

/// `true` when `CQLITE_REQUIRE_FIXTURES` is set to a truthy value. In strict
/// mode (the full gate's default), a `test_tomb` fixture that would otherwise
/// SKIP because its binaries are unfetched must PANIC instead, so a CI gate
/// cannot false-pass on missing data (issue #972).
fn require_fixtures_strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

/// SKIP-clean (or, under `CQLITE_REQUIRE_FIXTURES=1`, PANIC-naming-the-table)
/// generation-dir resolution for the `test_tomb` fixture this file's
/// `dropped-column` case uses. Unlike `trace_decisions_generation_dir` above
/// (`test_explain`, fully git-committed including `*-Data.db`), `test_tomb`'s
/// binaries are NOT committed — `test-data/datasets/sstables/test_tomb` ships
/// only `.jsonl`/`.txt` sidecars — so an unfetched dataset root is a
/// legitimate absence, not a test-data defect (issue #4193 review finding;
/// precedent: `issue_1014_resurrection_safety_parity.rs::require_fixture`).
fn skip_clean_generation_dir(keyspace: &str, table: &str) -> Option<PathBuf> {
    let dir = datasets_root::sstables_root_for_table(keyspace, table).and_then(|root| {
        datasets_root::table_generation_dirs(&root, keyspace, table)
            .into_iter()
            .next()
    });
    if dir.is_none() {
        let reason = datasets_root::describe_search(keyspace, table);
        if require_fixtures_strict() {
            panic!(
                "CQLITE_REQUIRE_FIXTURES=1 but {keyspace}.{table} fixture is absent — {reason}; \
                 fetch it (bash test-data/scripts/fetch-datasets.sh)"
            );
        }
        eprintln!("[skip] {keyspace}.{table}: {reason}");
    }
    dir
}

/// Drive a traced FULL-COMPACTION merge (design.md §D3: every generation,
/// `with_now_secs`/`with_gc_before_secs`/`with_purge_safe(true)`) over
/// `test_explain.trace_decisions` for partition key `id` at `now`, and return
/// the recorded cells/tombstones. Fails closed (via `expect`) rather than
/// returning an empty trail on any construction error (R4).
fn explain_partition(id: i32, now: i64) -> (Vec<CellDecision>, Vec<TombstoneRecord>) {
    let schema = trace_decisions_schema();
    let dir = trace_decisions_generation_dir();
    let paths = discover_generations_newest_first(&dir);
    assert_eq!(
        paths.len(),
        2,
        "expected both nb-1-big and nb-2-big under {}",
        dir.display()
    );
    let effective_schema = effective_compaction_schema(&schema, &paths);
    let gc_before = Some(now - GC_GRACE_SECONDS);
    let key = encode_partition_key_columns(&[Value::Integer(id)], &effective_schema)
        .expect("encode partition key");
    let merger = build_single_partition_merger_with_trace(
        paths,
        &[key],
        &effective_schema,
        ScanCancel::default(),
        RecordingSink::new(),
    )
    .expect("build traced merger")
    .unwrap_or_else(|| panic!("partition id={id} must be held by at least one generation"));
    let mut merger = merger
        .with_now_secs(Some(now))
        .with_gc_before_secs(gc_before)
        .with_purge_safe(true);
    loop {
        match merger.step().expect("merge step") {
            MergeStep::Complete => break,
            MergeStep::Partition { .. } => {}
        }
    }
    let (cells, tombstones, _probes) = merger.into_trace_sink().into_parts();
    (cells, tombstones)
}

fn ck(value: i32) -> ClusteringKey {
    ClusteringKey::single("ck", Value::Integer(value))
}

fn only_cell<'a>(
    cells: &'a [CellDecision],
    column: &str,
    clustering: i32,
    run_index: usize,
) -> &'a CellDecision {
    let matches: Vec<&CellDecision> = cells
        .iter()
        .filter(|c| {
            c.column == column && c.clustering == Some(ck(clustering)) && c.run_index == run_index
        })
        .collect();
    assert_eq!(
        matches.len(),
        1,
        "expected exactly one CellDecision for column={column} clustering={clustering} \
         run_index={run_index}, found {}: {cells:?}",
        matches.len()
    );
    matches[0]
}

// ===========================================================================
// Partition 1 (id=1): shadowed-by-timestamp
// A `v='older'` (ts=1000, nb-1-big) is shadowed by B `v='newer'` (ts=2000,
// nb-2-big) — strictly higher timestamp wins (Cells#reconcile). run_index 0 =
// nb-2-big (newest), run_index 1 = nb-1-big.
// ===========================================================================

#[test]
fn verdict_shadowed_by_timestamp() {
    let (cells, _tombstones) = explain_partition(1, NOW_ORDINARY);
    let loser = only_cell(&cells, "v", 1, 1);
    assert_eq!(loser.verdict, Verdict::ShadowedByTimestamp);
    assert_eq!(loser.value, Some(Value::text("older".to_string())));
    match &loser.decided_by {
        DecidedBy::Winner {
            run_index,
            writetime,
        } => {
            assert_eq!(*run_index, 0, "decided by the newer generation (nb-2-big)");
            assert_eq!(*writetime, 2000);
        }
        other => panic!("expected DecidedBy::Winner, got {other:?}"),
    }

    let winner = only_cell(&cells, "v", 1, 0);
    assert_eq!(winner.verdict, Verdict::Winner);
    assert_eq!(winner.value, Some(Value::text("newer".to_string())));
}

// ===========================================================================
// Partition 2 (id=2): shadowed-by-tombstone{row}
// A `v='row-shadowed'` (ts=1000) is shadowed by B's ROW deletion at ts=2000,
// ldt=GEN_B_LDT (nb-2-big-Data.db.jsonl: row-scope `deletion_info`).
// ===========================================================================

#[test]
fn verdict_shadowed_by_tombstone_row() {
    let (cells, tombstones) = explain_partition(2, NOW_ORDINARY);
    let loser = only_cell(&cells, "v", 1, 1);
    assert_eq!(
        loser.verdict,
        Verdict::ShadowedByTombstone(TombstoneKind::Row)
    );
    assert_eq!(loser.value, Some(Value::text("row-shadowed".to_string())));
    match &loser.decided_by {
        DecidedBy::Tombstone {
            kind,
            run_index,
            deletion_time,
            local_deletion_time,
            droppable_at_now,
        } => {
            assert_eq!(*kind, TombstoneKind::Row);
            assert_eq!(*run_index, 0);
            assert_eq!(*deletion_time, 2000);
            assert_eq!(*local_deletion_time, GEN_B_LDT);
            assert!(
                !droppable_at_now,
                "inside gc_grace at now={NOW_ORDINARY}, must not be droppable"
            );
        }
        other => panic!("expected DecidedBy::Tombstone, got {other:?}"),
    }

    let row_tombstone = tombstones
        .iter()
        .find(|t| t.kind == TombstoneKind::Row && t.run_index == 0)
        .unwrap_or_else(|| panic!("expected a row tombstone record, got {tombstones:?}"));
    assert_eq!(row_tombstone.deletion_time, 2000);
    assert_eq!(row_tombstone.local_deletion_time, GEN_B_LDT);
    assert!(!row_tombstone.droppable_at_now);
}

// ===========================================================================
// Partition 3 (id=3): shadowed-by-tombstone{range} + a winner outside the
// range. A ck=1 `v='range-shadowed'` is covered by B's inclusive range [1,2]
// at ts=2000, ldt=GEN_B_LDT; A ck=3 `v='outside-range'` survives (outside the
// range) with no competing version, so it is the WINNER.
// ===========================================================================

#[test]
fn verdict_shadowed_by_tombstone_range_and_winner_outside_range() {
    let (cells, tombstones) = explain_partition(3, NOW_ORDINARY);

    let range_tombstone = tombstones
        .iter()
        .find(|t| t.kind == TombstoneKind::Range)
        .unwrap_or_else(|| panic!("expected a range tombstone record, got {tombstones:?}"));
    assert_eq!(range_tombstone.run_index, 0);
    assert_eq!(range_tombstone.deletion_time, 2000);
    assert_eq!(range_tombstone.local_deletion_time, GEN_B_LDT);
    assert!(!range_tombstone.droppable_at_now);

    let covered = only_cell(&cells, "v", 1, 1);
    assert_eq!(
        covered.verdict,
        Verdict::ShadowedByTombstone(TombstoneKind::Range)
    );
    assert_eq!(
        covered.value,
        Some(Value::text("range-shadowed".to_string()))
    );

    let outside = only_cell(&cells, "v", 3, 1);
    assert_eq!(outside.verdict, Verdict::Winner);
    assert_eq!(
        outside.value,
        Some(Value::text("outside-range".to_string()))
    );
}

// ===========================================================================
// Partition 4 (id=4): shadowed-by-tombstone{cell} at an EQUAL timestamp.
// A `v='equal-timestamp'` at ts=1000 (nb-1-big); B's `DELETE v ... USING
// TIMESTAMP 1000` (nb-2-big) — SAME timestamp. Cassandra `Cells#reconcile`:
// at equal timestamp a DELETION beats a live cell, so the tombstone wins and
// A's live cell is `shadowed-by-tombstone{cell}` (not merely `-timestamp`,
// per design.md §D1 step 1's equal-ts carve-out).
// ===========================================================================

#[test]
fn verdict_shadowed_by_tombstone_cell_at_equal_timestamp() {
    let (cells, tombstones) = explain_partition(4, NOW_ORDINARY);
    let loser = only_cell(&cells, "v", 1, 1);
    assert_eq!(
        loser.verdict,
        Verdict::ShadowedByTombstone(TombstoneKind::Cell)
    );
    assert_eq!(
        loser.value,
        Some(Value::text("equal-timestamp".to_string()))
    );
    match &loser.decided_by {
        DecidedBy::Tombstone {
            kind,
            run_index,
            deletion_time,
            local_deletion_time,
            droppable_at_now,
        } => {
            assert_eq!(*kind, TombstoneKind::Cell);
            assert_eq!(*run_index, 0);
            assert_eq!(*deletion_time, 1000, "the tombstone's OWN write timestamp");
            assert_eq!(*local_deletion_time, GEN_B_LDT);
            assert!(!droppable_at_now);
        }
        other => panic!("expected DecidedBy::Tombstone, got {other:?}"),
    }

    let cell_tombstone = tombstones
        .iter()
        .find(|t| t.kind == TombstoneKind::Cell && t.column.as_deref() == Some("v"))
        .unwrap_or_else(|| panic!("expected a cell tombstone record, got {tombstones:?}"));
    assert_eq!(cell_tombstone.deletion_time, 1000);
    assert_eq!(cell_tombstone.local_deletion_time, GEN_B_LDT);
    assert!(!cell_tombstone.droppable_at_now);
}

// ===========================================================================
// Partition 5 (id=5): shadowed-by-tombstone{partition}.
// A `v='partition-shadowed'` (ts=1000) is shadowed by B's PARTITION deletion
// at ts=2000, ldt=GEN_B_LDT (nb-2-big-Data.db.jsonl: partition-scope
// `deletion_info`).
// ===========================================================================

#[test]
fn verdict_shadowed_by_tombstone_partition() {
    let (cells, tombstones) = explain_partition(5, NOW_ORDINARY);
    let loser = only_cell(&cells, "v", 1, 1);
    assert_eq!(
        loser.verdict,
        Verdict::ShadowedByTombstone(TombstoneKind::Partition)
    );
    assert_eq!(
        loser.value,
        Some(Value::text("partition-shadowed".to_string()))
    );
    match &loser.decided_by {
        DecidedBy::Tombstone {
            kind,
            run_index,
            deletion_time,
            local_deletion_time,
            droppable_at_now,
        } => {
            assert_eq!(*kind, TombstoneKind::Partition);
            assert_eq!(*run_index, 0);
            assert_eq!(*deletion_time, 2000);
            assert_eq!(*local_deletion_time, GEN_B_LDT);
            assert!(!droppable_at_now);
        }
        other => panic!("expected DecidedBy::Tombstone, got {other:?}"),
    }

    let partition_tombstone = tombstones
        .iter()
        .find(|t| t.kind == TombstoneKind::Partition)
        .unwrap_or_else(|| panic!("expected a partition tombstone record, got {tombstones:?}"));
    assert_eq!(partition_tombstone.run_index, 0);
    assert_eq!(partition_tombstone.deletion_time, 2000);
    assert_eq!(partition_tombstone.local_deletion_time, GEN_B_LDT);
    assert!(!partition_tombstone.droppable_at_now);
}

/// `now` far enough past `GEN_B_LDT + GC_GRACE_SECONDS` that a tombstone with
/// local_deletion_time == GEN_B_LDT is genuinely droppable. Every
/// `assert!(!droppable_at_now)` above is pinned at `NOW_ORDINARY` (inside
/// gc_grace, per design.md §D6), where `droppable_at_now == false` regardless
/// of whether the code even LOOKS at `now` — so this constant exists to make
/// those assertions load-bearing: it is used below to prove `droppable_at_now`
/// genuinely flips to `true` once gc_grace has elapsed (issue #4193 review
/// finding — `trace_entry_shadowed` previously hardcoded `false`
/// unconditionally, which every `NOW_ORDINARY` case above could not catch).
const NOW_PAST_GC_GRACE: i64 = GEN_B_LDT as i64 + GC_GRACE_SECONDS + 1;

// ===========================================================================
// Row / range / partition tombstones ARE droppable once gc_grace has elapsed.
// Reuses partitions 2 (row), 3 (range) and 5 (partition) at `NOW_PAST_GC_GRACE`
// instead of `NOW_ORDINARY` — same fixtures, same tombstones, only `now`
// differs — so a regression that hardcodes `droppable_at_now: false` (or
// otherwise stops consulting `gc_before`) fails this test even though every
// `NOW_ORDINARY` assertion above stays green.
// ===========================================================================

#[test]
fn verdict_row_tombstone_is_droppable_once_gc_grace_has_elapsed() {
    let (cells, tombstones) = explain_partition(2, NOW_PAST_GC_GRACE);
    let loser = only_cell(&cells, "v", 1, 1);
    match &loser.decided_by {
        DecidedBy::Tombstone {
            droppable_at_now, ..
        } => assert!(
            *droppable_at_now,
            "past gc_grace at now={NOW_PAST_GC_GRACE}, must be droppable"
        ),
        other => panic!("expected DecidedBy::Tombstone, got {other:?}"),
    }
    let row_tombstone = tombstones
        .iter()
        .find(|t| t.kind == TombstoneKind::Row && t.run_index == 0)
        .unwrap_or_else(|| panic!("expected a row tombstone record, got {tombstones:?}"));
    assert!(row_tombstone.droppable_at_now);
}

#[test]
fn verdict_range_tombstone_is_droppable_once_gc_grace_has_elapsed() {
    let (_cells, tombstones) = explain_partition(3, NOW_PAST_GC_GRACE);
    let range_tombstone = tombstones
        .iter()
        .find(|t| t.kind == TombstoneKind::Range)
        .unwrap_or_else(|| panic!("expected a range tombstone record, got {tombstones:?}"));
    assert!(range_tombstone.droppable_at_now);
}

#[test]
fn verdict_partition_tombstone_is_droppable_once_gc_grace_has_elapsed() {
    let (cells, tombstones) = explain_partition(5, NOW_PAST_GC_GRACE);
    let loser = only_cell(&cells, "v", 1, 1);
    match &loser.decided_by {
        DecidedBy::Tombstone {
            droppable_at_now, ..
        } => assert!(
            *droppable_at_now,
            "past gc_grace at now={NOW_PAST_GC_GRACE}, must be droppable"
        ),
        other => panic!("expected DecidedBy::Tombstone, got {other:?}"),
    }
    let partition_tombstone = tombstones
        .iter()
        .find(|t| t.kind == TombstoneKind::Partition)
        .unwrap_or_else(|| panic!("expected a partition tombstone record, got {tombstones:?}"));
    assert!(partition_tombstone.droppable_at_now);
}

// ===========================================================================
// Partition 6 (id=6): shadowed-by-tombstone{collection}.
// A full-map overwrite (`UPDATE ... SET m={'new':2}`) writes a COMPLEX
// DELETION marker at `write_timestamp - 1` alongside the new element
// (Cassandra `cql3/UpdateParameters.java#setComplexDeletionTimeForOverwrite`,
// verified against the pinned `cassandra-5.0.8` tag per the handoff). Gen A's
// element `m['old']=1` (ts=1000) carries its own (superseded) marker at
// mfda=999; gen B's element `m['new']=2` (ts=2000) carries the marker at
// mfda=1999. `apply_complex_deletions`'s strict-supersede keeps the greater
// mfda (1999, from gen B) as the ACTIVE marker; `old`'s ts(1000) <= mfda(1999)
// is shadowed, `new`'s ts(2000) > mfda(1999) survives as the winner.
// ===========================================================================

#[test]
fn verdict_shadowed_by_tombstone_collection() {
    let (cells, tombstones) = explain_partition(6, NOW_ORDINARY);

    let shadowed = only_cell(&cells, "m", 1, 1);
    assert_eq!(
        shadowed.verdict,
        Verdict::ShadowedByTombstone(TombstoneKind::Collection)
    );
    match &shadowed.decided_by {
        DecidedBy::Tombstone {
            kind,
            deletion_time,
            local_deletion_time,
            droppable_at_now,
            ..
        } => {
            assert_eq!(*kind, TombstoneKind::Collection);
            assert_eq!(
                *deletion_time, 1999,
                "the ACTIVE (strict-max) marker's mfda, from gen B, not gen A's superseded 999"
            );
            assert_eq!(*local_deletion_time, GEN_B_LDT);
            assert!(!droppable_at_now);
        }
        other => panic!("expected DecidedBy::Tombstone, got {other:?}"),
    }

    let winner = only_cell(&cells, "m", 1, 0);
    assert_eq!(winner.verdict, Verdict::Winner);

    let collection_tombstone = tombstones
        .iter()
        .find(|t| t.kind == TombstoneKind::Collection)
        .unwrap_or_else(|| panic!("expected a collection tombstone record, got {tombstones:?}"));
    assert_eq!(collection_tombstone.deletion_time, 1999);
    assert_eq!(collection_tombstone.local_deletion_time, GEN_B_LDT);
}

// ===========================================================================
// Partition 7 (id=7): expired.
// A `v='expires'` (ts=1000) carries TTL=3600, on-disk `expires_at` (LDT) =
// 1789966734 (nb-1-big-Data.db.jsonl). Evaluated one second after expiry
// (`now=1789966735`): `AbstractCell.isLive` is `nowInSec < localDeletionTime`,
// so `now == expires_at` is ALREADY expired and `now == expires_at + 1` is
// unambiguously so.
// ===========================================================================

#[test]
fn verdict_expired() {
    const EXPIRES_AT: i64 = 1_789_966_734;
    let (cells, _tombstones) = explain_partition(7, EXPIRES_AT + 1);
    let cell = only_cell(&cells, "v", 1, 1);
    assert_eq!(cell.verdict, Verdict::Expired);
    assert_eq!(cell.ttl, Some(3600));
    assert_eq!(cell.expires_at, Some(EXPIRES_AT));
    match &cell.decided_by {
        DecidedBy::Expiry { expires_at, now } => {
            assert_eq!(*expires_at, EXPIRES_AT);
            assert_eq!(*now, EXPIRES_AT + 1);
        }
        other => panic!("expected DecidedBy::Expiry, got {other:?}"),
    }
}

// ===========================================================================
// Partition 8 (id=8) + R2.3: purgeable, exact to the second.
// A `v='purge-boundary'` (ts=1000) is shadowed-by-timestamp by B's cell
// tombstone (`DELETE v`, ts=2000, ldt=GEN_B_LDT). The winning cell tombstone
// itself is retained at `now = ldt + gc_grace` (`purge_gc_grace`'s strict `<`
// gate: `ldt < gc_before` is false when equal) and purged
// (`Verdict::Purgeable`) one second later (`db/partitions/PurgeFunction.java`
// strict `localDeletionTime < gcBefore`, issue #1385).
// ===========================================================================

#[test]
fn verdict_purgeable_gc_grace_boundary_exact_to_the_second() {
    let boundary_now = i64::from(GEN_B_LDT) + GC_GRACE_SECONDS;

    // At the boundary: retained. The always-emitted TombstoneRecord (from
    // resolve_cell_winners, independent of purge) says so via
    // `droppable_at_now`, and NO Purgeable CellDecision is emitted for `v`.
    let (cells, tombstones) = explain_partition(8, boundary_now);
    let cell_tombstone = tombstones
        .iter()
        .find(|t| t.kind == TombstoneKind::Cell && t.column.as_deref() == Some("v"))
        .unwrap_or_else(|| panic!("expected a cell tombstone record, got {tombstones:?}"));
    assert_eq!(cell_tombstone.deletion_time, 2000);
    assert_eq!(cell_tombstone.local_deletion_time, GEN_B_LDT);
    assert!(
        !cell_tombstone.droppable_at_now,
        "ldt == gcBefore must NOT be droppable (strict `<`, #1385)"
    );
    assert!(
        !cells
            .iter()
            .any(|c| c.column == "v" && c.verdict == Verdict::Purgeable),
        "no cell may be Purgeable at the retained boundary, got {cells:?}"
    );

    // One second later: purged. The winning cell tombstone (run_index 0,
    // nb-2-big) now emits Verdict::Purgeable.
    let (cells, tombstones) = explain_partition(8, boundary_now + 1);
    let cell_tombstone = tombstones
        .iter()
        .find(|t| t.kind == TombstoneKind::Cell && t.column.as_deref() == Some("v"))
        .unwrap_or_else(|| panic!("expected a cell tombstone record, got {tombstones:?}"));
    assert!(
        cell_tombstone.droppable_at_now,
        "ldt == gcBefore - 1 must be droppable one second past the boundary"
    );
    let purged = cells
        .iter()
        .find(|c| c.column == "v" && c.verdict == Verdict::Purgeable)
        .unwrap_or_else(|| panic!("expected a Purgeable CellDecision for `v`, got {cells:?}"));
    assert_eq!(purged.run_index, 0);
    match &purged.decided_by {
        DecidedBy::GcGrace {
            ldt,
            gc_before,
            now,
        } => {
            assert_eq!(*ldt, GEN_B_LDT);
            assert_eq!(*gc_before, boundary_now + 1 - GC_GRACE_SECONDS);
            assert_eq!(*now, boundary_now + 1);
        }
        other => panic!("expected DecidedBy::GcGrace, got {other:?}"),
    }
}

// ===========================================================================
// dropped-column: `test_tomb/dropped_regular_col` (design.md §D6).
// Gen 1 (nb-1-big) writes BOTH `keep_col` and `drop_col` at ts=1609459200000000
// (T_GEN1, micros, tombstone-parity.cql's documented fixed timestamp scheme)
// for ck=1..3, then `drop_col` is ALTERed away; gen 2 (nb-2-big) writes only
// `keep_col` for ck=4..6 (test-data/scripts/generate-tombstone-parity.sh:659,
// `ALTER TABLE dropped_regular_col DROP drop_col;`).
//
// The ALTER runs with NO `USING TIMESTAMP`, so its real drop time is
// Cassandra's wall clock at fixture-GENERATION time (~2026) — a value this
// repository does not capture reproducibly (`sstablemetadata`'s text dump
// carries no dropped-columns section for this Cassandra version, confirmed
// against both generations' `-Statistics.db.txt`). `dropped_columns` is
// SCHEMA metadata (production reads it from `self.schema.dropped_columns`,
// `mod.rs:2431` — never derived per-SSTable from `Statistics.db`, and
// `effective_compaction_schema` re-adds only STATIC columns, never populates
// this map), and the committed `.cql` schema format has no drop-column
// syntax to express it (`schema::cql_parser::parse_create_table` always
// returns an empty `dropped_columns`). So — following the SAME precedent
// `issue_1015_dropped_static_parity.rs::dropped_regular_schema` already
// established for this exact fixture family — the schema declares
// `drop_col` dropped at `T_GEN2` (tombstone-parity.cql's own next fixed
// timestamp after `T_GEN1`, so it is unambiguously later than every gen-1
// cell this test asserts is dropped): this is supplying the authoritative
// schema-level fact a real deployment would read from
// `system_schema.dropped_columns`, not inferring anything from the cell
// bytes (no-heuristics mandate).
// ===========================================================================

#[test]
fn verdict_dropped_column() {
    const T_GEN1_MICROS: i64 = 1_609_459_200_000_000;
    const T_GEN2_MICROS: i64 = 1_609_545_600_000_000;

    let schema_path = datasets_root::schema_path("tombstone-parity.cql")
        .expect("committed test-data/schemas/tombstone-parity.cql fixture");
    let mut schema = load_table_schema(&schema_path, "test_tomb", "dropped_regular_col");
    schema
        .dropped_columns
        .insert("drop_col".to_string(), T_GEN2_MICROS);

    let Some(dir) = skip_clean_generation_dir("test_tomb", "dropped_regular_col") else {
        return;
    };
    let paths = discover_generations_newest_first(&dir);
    assert_eq!(paths.len(), 2, "expected two generations under {dir:?}");

    let effective_schema = effective_compaction_schema(&schema, &paths);
    let drop_time = T_GEN2_MICROS;

    let key = encode_partition_key_columns(&[Value::Integer(1)], &effective_schema)
        .expect("encode partition key");
    let merger = build_single_partition_merger_with_trace(
        paths,
        &[key],
        &effective_schema,
        ScanCancel::default(),
        RecordingSink::new(),
    )
    .expect("build traced merger")
    .expect("pk=1 must be held by the fixture's first generation");
    // Full-compaction posture (design.md §D3); gc_grace is irrelevant to the
    // dropped-column step, which runs independently of gc-grace purging, so
    // a permissive gc_before never masks the result.
    let mut merger = merger
        .with_now_secs(Some(T_GEN1_MICROS / 1_000_000 + 10_000_000))
        .with_purge_safe(true);
    loop {
        match merger.step().expect("merge step") {
            MergeStep::Complete => break,
            MergeStep::Partition { .. } => {}
        }
    }
    let (cells, _tombstones, _probes) = merger.into_trace_sink().into_parts();

    let dropped: Vec<&CellDecision> = cells
        .iter()
        .filter(|c| c.column == "drop_col" && c.verdict == Verdict::DroppedColumn)
        .collect();
    assert_eq!(
        dropped.len(),
        3,
        "expected all three gen-1 `drop_col` cells (ck=1,2,3) to be dropped-column, got {cells:?}"
    );
    for cell in dropped {
        assert_eq!(cell.decided_by, DecidedBy::DropTime(drop_time));
        assert_eq!(
            cell.run_index, 1,
            "the dropped cells came from the older generation"
        );
    }

    // `keep_col` must never be affected by the drop — a phantom-purge would
    // be exactly the no-heuristics/authoritative-metadata failure this test
    // guards against.
    assert!(
        !cells
            .iter()
            .any(|c| c.column == "keep_col" && c.verdict == Verdict::DroppedColumn),
        "keep_col must never be reported dropped-column"
    );
}

/// R2: the verdict vocabulary is closed. This does not (and per the owner's
/// #3725 ruling, R2.1 does not either) prove there is exactly one emit SITE
/// per verdict in the source — it proves BEHAVIORALLY that every verdict this
/// fixture set can produce is one of the seven named variants, by construction
/// of the `Verdict` enum itself (`match` exhaustiveness at compile time means
/// no other variant CAN be constructed).
#[test]
fn verdict_enum_is_exhaustively_the_seven_named_variants() {
    fn assert_closed(verdict: &Verdict) {
        match verdict {
            Verdict::Winner
            | Verdict::ShadowedByTimestamp
            | Verdict::ShadowedByTombstone(_)
            | Verdict::Expired
            | Verdict::Purgeable
            | Verdict::DroppedColumn => {}
        }
    }
    let (cells, _tombstones) = explain_partition(1, NOW_ORDINARY);
    for cell in &cells {
        assert_closed(&cell.verdict);
    }
    // Compile-time closure check independent of any fixture: constructing a
    // value of each variant proves the enum has exactly these members (an
    // added variant would need a new arm above or this file fails to build
    // under `-D unused` from the newly-unreachable... — no: it would simply
    // compile silently, which is why the REAL closure guarantee is the
    // exhaustive `match` above, not this list).
    let _all = [
        Verdict::Winner,
        Verdict::ShadowedByTimestamp,
        Verdict::ShadowedByTombstone(TombstoneKind::Partition),
        Verdict::Expired,
        Verdict::Purgeable,
        Verdict::DroppedColumn,
    ];
    let _ = ProbeOutcome::Hit; // keep the trace-probe type reachable in this file
}
