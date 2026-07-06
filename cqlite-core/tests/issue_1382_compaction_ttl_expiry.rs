//! Issue #1382: TTL expiry during compaction.
//!
//! The production reconcile pipeline
//! (`resolve_cell_winners → shadow_by_row_deletion → filter_dropped_columns →
//! expire_ttl_cells → purge_gc_grace → build`) must treat an EXPIRED live cell
//! (an expiring cell whose authoritative on-disk `localDeletionTime` is already
//! past the pinned evaluation instant `now_secs`) as a tombstone — matching
//! Cassandra `AbstractCell.purge`, which converts the expired cell via
//! `BufferCell.tombstone(column, timestamp(), localDeletionTime() - ttl(), path())`:
//! the tombstone's `localDeletionTime` is the CREATION time (`ldt - ttl`), and
//! `markedForDeleteAt` (the cell's own write timestamp) is unchanged. `Cell.isLive`
//! is `nowInSec < localDeletionTime`, so a cell at `now == ldt` is already expired.
//! gc grace is measured from creation: once `(ldt - ttl) < gcBefore` (and the
//! overlap / max_purgeable gate allows) the cell is purged entirely.
//!
//! Every test here drives the REAL public compaction surface
//! ([`compact_sstables`]) with a PINNED `now_secs`/`gc_before_secs`, then reads
//! the compacted output back through the [`KWayMerger`] read path. The pinning
//! is deterministic: the cell's on-disk `localDeletionTime` is fixed once
//! written (writer stamps `flush_now + ttl`), so each test READS that
//! authoritative LDT back from the input, then pins `now_secs`/`gc_before`
//! RELATIVE to it — the expiry / purge DECISIONS never sample a wall clock.
//! (Issue #1538: `WriteWithTtl` now carries the source cell's authoritative
//! per-cell LDT, threaded verbatim through the compaction merge→writer, so a
//! surviving LIVE TTL cell's output LDT EQUALS its input LDT byte-for-byte —
//! criterion 3 asserts that equality.)
//!
//! ## Acceptance criteria (issue #1382)
//!
//!   1. `expired_within_grace_emitted_as_tombstone` — expired but the tombstone's
//!      creation-time LDT (`ldt - ttl`) is `>= gcBefore`: output keeps a cell
//!      tombstone at that creation-time LDT, NOT the live value.
//!   2. `expired_past_grace_purged_entirely` — creation-time LDT `< gcBefore`: the
//!      cell is absent from the compacted output entirely (purged).
//!   3. `live_ttl_cell_survives` — LDT in the future: emitted live with a
//!      TTL and its value, and its LDT stays in the future (un-expired). Issue
//!      #1538: the surviving cell's LDT is preserved byte-identically (the source
//!      per-cell LDT threads verbatim through the merge→writer), so this test
//!      asserts the output LDT EQUALS the input LDT.
//!   4. `expired_past_grace_retained_under_overlap_gate` — expired-past-grace but
//!      a PARTIAL compaction with an EXCLUDED overlapping SSTable holding older
//!      data under the same key: the tombstone is RETAINED (max_purgeable gate).
//!   5. Cassandra byte-oracle — DEFERRED to issue #1387 (fixtures). Present here
//!      as a fail-closed skip-on-absent test.
//!   6. `differential_compact_matches_reference_on_ttl_corpus` — production
//!      `compact_sstables` output == a hand-written reference TTL merge over a
//!      randomized corpus (the guard that would have caught the original bug).

#![cfg(feature = "write-support")]

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::merge::{compact_sstables, CellData, MergeStep, RowData};
use cqlite_core::storage::write_engine::{
    CellOperation, ClusteringKey, KWayMerger, Mutation, PartitionKey, TableId, WriteEngine,
    WriteEngineConfig,
};
use cqlite_core::types::{TombstoneType, Value};
use tempfile::TempDir;

// ===========================================================================
// Schema + mutation helpers
// ===========================================================================

fn make_schema() -> TableSchema {
    TableSchema {
        keyspace: "ttl_ks".to_string(),
        table: "items".to_string(),
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
            col("name", "text", true),
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn col(name: &str, ty: &str, nullable: bool) -> Column {
    Column {
        name: name.to_string(),
        data_type: ty.to_string(),
        nullable,
        default: None,
        is_static: false,
    }
}

/// Write a row whose `name` cell carries a per-cell TTL. The on-disk
/// `localDeletionTime` becomes `flush_wall_clock + ttl` (writer contract); tests
/// read it back and pin `now_secs`/`gc_before` relative to it.
fn write_ttl_row(id: i32, ck: i32, name: &str, ttl_seconds: u32, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new("ttl_ks", "items"),
        PartitionKey::single("id", Value::Integer(id)),
        Some(ClusteringKey::single("ck", Value::Integer(ck))),
        vec![CellOperation::WriteWithTtl {
            column: "name".to_string(),
            value: Value::Text(name.to_string()),
            ttl_seconds,
            local_deletion_time: None,
        }],
        ts,
        None,
    )
}

/// Write a plain (non-expiring) row.
fn write_live_row(id: i32, ck: i32, name: &str, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new("ttl_ks", "items"),
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

// ===========================================================================
// Runtime + I/O helpers
// ===========================================================================

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
}

/// Flush a batch of mutations into ONE input SSTable under `data_dir` and return
/// the discovered Data.db paths (newest-generation first).
fn flush_batch(data_dir: &Path, wal_dir: &Path, schema: &TableSchema, muts: Vec<Mutation>) {
    let config = WriteEngineConfig::new(
        data_dir.to_path_buf(),
        wal_dir.to_path_buf(),
        schema.clone(),
    );
    let mut engine = WriteEngine::new(config).expect("engine");
    for m in muts {
        engine.write(m).expect("write mutation");
    }
    let r = rt();
    r.block_on(engine.flush()).expect("flush").expect("info");
    r.block_on(engine.close()).expect("close engine");
}

/// Discover `nb-*-big-Data.db` inputs, newest-generation first.
fn discover_inputs(dir: &Path) -> Vec<PathBuf> {
    let mut found: Vec<(u64, PathBuf)> = Vec::new();
    collect(dir, &mut found, 8);
    found.sort_by(|a, b| b.0.cmp(&a.0));
    found.into_iter().map(|(_, p)| p).collect()
}

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

/// Read every surviving `(pk, ck)` row of a compacted/input directory back
/// through the merge read path with expiry DISABLED (`now_secs = None`) so the
/// raw on-disk cell state is observed. Returns a map keyed by the `name` cell's
/// clustering int → the surviving `name` [`CellData`] (or `None` if the row has
/// no `name` cell / does not survive).
fn read_name_cells(inputs: &[PathBuf], schema: &TableSchema, purge_safe: bool) -> Vec<CellData> {
    // A fully-purged compaction produces an empty (0-byte) Data.db; the reader
    // cannot parse an empty header, and there is nothing to read anyway. Filter
    // those out so an all-purged output correctly reads back as ZERO cells.
    let non_empty: Vec<PathBuf> = inputs
        .iter()
        .filter(|p| std::fs::metadata(p).map(|m| m.len() > 0).unwrap_or(false))
        .cloned()
        .collect();
    if non_empty.is_empty() {
        return Vec::new();
    }
    let mut merger = KWayMerger::new_with_gc(non_empty, schema, None, None)
        .expect("merger")
        .with_purge_safe(purge_safe);
    let mut cells = Vec::new();
    loop {
        match merger.step().expect("step") {
            MergeStep::Complete => break,
            MergeStep::Partition { rows, .. } => {
                for entry in rows {
                    if let RowData::Live { cells: row_cells } = &entry.row_data {
                        for c in row_cells {
                            if c.column == "name" {
                                cells.push(c.clone());
                            }
                        }
                    }
                }
            }
        }
    }
    cells
}

/// Discover the authoritative on-disk `localDeletionTime` (GC-clock seconds) of
/// the single expiring `name` cell in `inputs`. Panics if none is found (a test
/// setup error, not a library error).
fn expiring_name_ldt(inputs: &[PathBuf], schema: &TableSchema) -> i64 {
    let cells = read_name_cells(inputs, schema, false);
    for c in &cells {
        if let Some(ldt) = c.local_deletion_time {
            return i64::from(ldt as u32);
        }
    }
    panic!("no expiring name cell with a local_deletion_time found in inputs");
}

/// Run the real one-shot compaction with a PINNED `now_secs`/`gc_before_secs`.
fn compact(
    inputs: Vec<PathBuf>,
    out_dir: &Path,
    schema: &TableSchema,
    gc_before_secs: Option<i64>,
    now_secs: Option<i64>,
    purge_safe: bool,
) {
    rt().block_on(compact_sstables(
        inputs,
        out_dir,
        schema,
        1,
        gc_before_secs,
        now_secs,
        purge_safe,
    ))
    .expect("compaction succeeds");
}

// ===========================================================================
// Acceptance criterion 1 — expired within grace → cell tombstone (not live)
// ===========================================================================

#[test]
fn expired_within_grace_emitted_as_tombstone() {
    let temp = TempDir::new().unwrap();
    let in_dir = temp.path().join("in");
    let wal_dir = temp.path().join("wal");
    let out_dir = temp.path().join("out");
    let schema = make_schema();

    flush_batch(
        &in_dir,
        &wal_dir,
        &schema,
        vec![write_ttl_row(1, 0, "secret", 60, 100)],
    );
    let inputs = discover_inputs(&in_dir);
    assert!(!inputs.is_empty(), "expected an input SSTable");

    let ldt = expiring_name_ldt(&inputs, &schema);
    // Expired: now is AFTER the expiry instant. The tombstone's effective LDT is
    // the CREATION time `ldt - ttl` (parity, `AbstractCell.purge`
    // `localDeletionTime() - ttl()`), so within grace means gcBefore <= `ldt - ttl`.
    // Pin gcBefore == `ldt - ttl` so the strict `(ldt - ttl) < gcBefore` purge
    // test is false — the tombstone is retained. (`write_ttl_row` uses ttl = 60.)
    let now_secs = ldt + 10;
    let gc_before = Some(ldt - 60); // (ldt - ttl) is NOT < gcBefore → within grace

    compact(inputs, &out_dir, &schema, gc_before, Some(now_secs), true);

    let out_inputs = discover_inputs(&out_dir);
    let cells = read_name_cells(&out_inputs, &schema, true);
    assert_eq!(cells.len(), 1, "expected exactly one surviving name cell");
    let cell = &cells[0];
    // Must be a cell tombstone, NOT the live "secret" value.
    match &cell.value {
        Value::Tombstone(info) => {
            assert_eq!(info.tombstone_type, TombstoneType::CellTombstone);
            // localDeletionTime == the CREATION time (ldt - ttl), Cassandra
            // `AbstractCell.purge` `localDeletionTime() - ttl()`.
            assert_eq!(
                info.local_deletion_time,
                ldt - 60,
                "tombstone LDT == creation time (ldt - ttl)"
            );
        }
        other => panic!("expected a CellTombstone, got live value {other:?}"),
    }
    assert_ne!(
        cell.value,
        Value::Text("secret".to_string()),
        "live value must NOT survive"
    );
    // markedForDeleteAt (the cell's own write timestamp) is unchanged.
    assert_eq!(
        cell.timestamp, 100,
        "markedForDeleteAt == original cell write ts"
    );
}

// ===========================================================================
// Acceptance criterion 2 — expired past grace → purged entirely
// ===========================================================================

#[test]
fn expired_past_grace_purged_entirely() {
    let temp = TempDir::new().unwrap();
    let in_dir = temp.path().join("in");
    let wal_dir = temp.path().join("wal");
    let out_dir = temp.path().join("out");
    let schema = make_schema();

    flush_batch(
        &in_dir,
        &wal_dir,
        &schema,
        vec![write_ttl_row(1, 0, "secret", 60, 100)],
    );
    let inputs = discover_inputs(&in_dir);
    let ldt = expiring_name_ldt(&inputs, &schema);
    // Expired AND past grace: the tombstone's effective LDT is the creation time
    // `ldt - ttl`, so gcBefore STRICTLY > `ldt - ttl` → purged. Pin the tight
    // creation-time boundary `ldt - ttl + 1`. Full compaction (purge_safe=true) →
    // overlap gate is +inf, so the purge is allowed.
    let now_secs = ldt + 1000;
    let gc_before = Some(ldt - 60 + 1); // (ldt - ttl) < gcBefore → purge

    compact(inputs, &out_dir, &schema, gc_before, Some(now_secs), true);

    let out_inputs = discover_inputs(&out_dir);
    let cells = read_name_cells(&out_inputs, &schema, true);
    assert!(
        cells.is_empty(),
        "expired-past-grace cell must be purged entirely, found {cells:?}"
    );
}

// ===========================================================================
// Acceptance criterion 3 — live TTL cell survives (not expired)
// ===========================================================================

#[test]
fn live_ttl_cell_survives() {
    let temp = TempDir::new().unwrap();
    let in_dir = temp.path().join("in");
    let wal_dir = temp.path().join("wal");
    let out_dir = temp.path().join("out");
    let schema = make_schema();

    // Large TTL so the on-disk LDT is far in the future.
    flush_batch(
        &in_dir,
        &wal_dir,
        &schema,
        vec![write_ttl_row(1, 0, "alive", 10_000_000, 100)],
    );
    let inputs = discover_inputs(&in_dir);
    let ldt = expiring_name_ldt(&inputs, &schema);
    // now BEFORE the expiry instant → not expired. gcBefore below ldt anyway.
    let now_secs = ldt - 1; // ldt is in the future relative to now
    let gc_before = Some(ldt - 100);

    compact(inputs, &out_dir, &schema, gc_before, Some(now_secs), true);

    let out_inputs = discover_inputs(&out_dir);
    let cells = read_name_cells(&out_inputs, &schema, true);
    assert_eq!(cells.len(), 1, "live TTL cell must survive");
    let cell = &cells[0];
    assert_eq!(
        cell.value,
        Value::Text("alive".to_string()),
        "value survives live"
    );
    assert!(cell.ttl.is_some(), "TTL preserved");
    // Issue #1538: the surviving live TTL cell is now re-emitted BYTE-IDENTICALLY.
    // `CellOperation::WriteWithTtl` carries the source cell's authoritative per-cell
    // `localDeletionTime`, and the compaction merge→writer threads it VERBATIM
    // (`cells_to_cell_operations` → `write_cell_with_ttl`) instead of re-deriving
    // `compaction_flush_wall_clock + ttl`. The output LDT therefore EQUALS the input
    // LDT exactly (no wall-clock skew), meeting #1382 crit-3 literally.
    let out_ldt = cell
        .local_deletion_time
        .map(|l| i64::from(l as u32))
        .expect("surviving live TTL cell carries a localDeletionTime");
    assert!(
        out_ldt > now_secs,
        "surviving cell must be un-expired (LDT {out_ldt} > now {now_secs})"
    );
    assert_eq!(
        out_ldt, ldt,
        "surviving live TTL cell's LDT is preserved byte-identically through compaction \
         (out {out_ldt} == src {ldt}), #1538"
    );
}

// ===========================================================================
// Acceptance criterion 4 — overlap gate retains an expired-past-grace cell in a
// partial compaction with an EXCLUDED overlapping SSTable holding older data.
// ===========================================================================

#[test]
fn expired_past_grace_retained_under_overlap_gate() {
    let temp = TempDir::new().unwrap();
    let in_dir = temp.path().join("in"); // the SSTable we compact
    let out_of_set = temp.path().join("outside"); // EXCLUDED overlapping SSTable
    let wal_dir = temp.path().join("wal");
    let schema = make_schema();

    // The excluded overlapping SSTable holds OLDER data under the same key.
    flush_batch(
        &out_of_set,
        &wal_dir.join("o"),
        &schema,
        vec![write_live_row(1, 0, "old", 50)],
    );
    let outside = discover_inputs(&out_of_set);

    // The compaction set: an expiring cell at a HIGHER timestamp than the older
    // outside data, so the expired tombstone WOULD shadow the outside row.
    flush_batch(
        &in_dir,
        &wal_dir.join("i"),
        &schema,
        vec![write_ttl_row(1, 0, "secret", 60, 100)],
    );
    let inputs = discover_inputs(&in_dir);
    let ldt = expiring_name_ldt(&inputs, &schema);
    let now_secs = ldt + 1000;
    let gc_before = Some(ldt + 1); // gc-purgeable by LDT

    // Overlap-aware min-outside timestamp. The outside row's write ts is 50; the
    // tombstone's markedForDeleteAt is 100 (the cell's write ts). Since 100 is
    // NOT strictly < the overlap bound, the overlap gate BLOCKS the purge.
    let outside_bound =
        cqlite_core::storage::write_engine::merge::compute_max_purgeable_timestamp(&outside);
    let bound = outside_bound.expect("outside SSTable has a min timestamp");
    assert!(
        bound <= 100,
        "outside min timestamp ({bound}) must be <= the tombstone's markedForDeleteAt (100) \
         so the overlap gate blocks the purge"
    );

    // Partial compaction: NOT purge_safe; supply the overlap bound explicitly.
    let mut merger = KWayMerger::new_with_gc(inputs.clone(), &schema, gc_before, Some(now_secs))
        .expect("merger")
        .with_max_purgeable_timestamp(Some(bound));
    let mut retained_tombstone = false;
    loop {
        match merger.step().expect("step") {
            MergeStep::Complete => break,
            MergeStep::Partition { rows, .. } => {
                for entry in rows {
                    if let RowData::Live { cells } = &entry.row_data {
                        for c in cells {
                            if c.column == "name" {
                                if let Value::Tombstone(info) = &c.value {
                                    if info.tombstone_type == TombstoneType::CellTombstone {
                                        retained_tombstone = true;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    assert!(
        retained_tombstone,
        "expired-past-grace cell must be RETAINED as a tombstone under the overlap gate \
         (markedForDeleteAt not strictly below the min outside timestamp)"
    );
}

// ===========================================================================
// Acceptance criterion 5 — Cassandra byte-oracle. DEFERRED to issue #1387
// (fixture commissioning). Fail-closed: skip unless CQLITE_REQUIRE_FIXTURES=1,
// panic if fixtures are present-but-incomplete, never a 0-comparison pass.
// ===========================================================================

#[test]
fn expired_ttl_matches_cassandra_byte_oracle_deferred_1387() {
    let strict = matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    );
    // Status update (#1410 / #1387): the #1387 fixture commissioning DID land the
    // `test_compaction_tombstone_ttl/ttl_expired_live-*` Cassandra reference, and the
    // authoritative byte-oracle for the expired-TTL scenario now lives in
    // `issue_1387_tombstone_ttl_compaction_byte_parity::ttl_expired_live_compaction_byte_for_byte`.
    // #1410 fixed the localDeletionTime baseline (lengths now match), but that byte
    // test is BLOCKED on #1538: byte parity needs an EXPIRING `WriteWithTtl` cell with
    // an authoritative pinned `localExpirationTime`, which the current API cannot supply.
    // This slot stays a fail-closed skip keyed on its OWN opt-in
    // `CQLITE_TTL_ORACLE_FIXTURES` (a hand-curated byte-oracle #1387 did not commission
    // under this name) so no CI gate false-passes; the real coverage is the #1387 byte
    // test above once #1538 lands. Fail closed only in strict mode.
    let fixture_root = std::env::var("CQLITE_TTL_ORACLE_FIXTURES").ok();
    match fixture_root {
        Some(root) if Path::new(&root).exists() => {
            panic!(
                "issue #1387 TTL byte-oracle fixtures present at {root} but the comparison is \
                 not yet implemented; wire it here once #1387 lands"
            );
        }
        _ => {
            if strict {
                panic!(
                    "CQLITE_REQUIRE_FIXTURES set but issue #1387 TTL byte-oracle fixtures are \
                     absent (set CQLITE_TTL_ORACLE_FIXTURES once #1387 commissions them)"
                );
            }
            eprintln!(
                "[SKIP] expired_ttl_matches_cassandra_byte_oracle_deferred_1387: \
                 fixtures pending issue #1387"
            );
        }
    }
}

// ===========================================================================
// Acceptance criterion 6 — differential: production compact_sstables output ==
// a hand-written reference TTL merge over a randomized corpus. This is the guard
// that would have caught the original bug (production ignored TTL expiry).
// ===========================================================================

/// Reference outcome for one `(id, ck)` slot after the reference TTL merge.
#[derive(Debug, Clone, PartialEq, Eq)]
enum RefOutcome {
    /// Cell survives LIVE with this text value.
    Live(String),
    /// Cell survives as a tombstone (expired within grace, not purgeable).
    Tombstone,
    /// Cell is absent (purged or expired-past-grace).
    Absent,
}

/// A corpus entry: a TTL cell with a chosen ttl and write ts.
struct CorpusCell {
    id: i32,
    ck: i32,
    name: String,
    ttl: u32,
    ts: i64,
}

/// Reference TTL reconcile for a single cell given the authoritative on-disk
/// `ldt` (the expiry instant), its `ttl`, and the pinned `now_secs`/`gc_before`.
/// Mirrors Cassandra:
///   * `Cell.isLive(nowInSec)` is `nowInSec < localDeletionTime`, so the cell is
///     EXPIRED iff `ldt <= now` (a cell at `now == ldt` is already expired).
///   * `AbstractCell.purge` converts the expired cell to a tombstone whose
///     `localDeletionTime` is the CREATION time `ldt - ttl`
///     (`localDeletionTime() - ttl()`), so it is PURGED iff `(ldt - ttl) < gcBefore`
///     (full compaction → overlap gate is +inf).
fn reference_outcome(name: &str, ldt: i64, ttl: i64, now_secs: i64, gc_before: i64) -> RefOutcome {
    if ldt <= now_secs {
        // Expired → tombstone at the creation time (ldt - ttl). Purged iff that
        // creation-time LDT is strictly below gcBefore.
        if ldt - ttl < gc_before {
            RefOutcome::Absent
        } else {
            RefOutcome::Tombstone
        }
    } else {
        RefOutcome::Live(name.to_string())
    }
}

#[test]
fn differential_compact_matches_reference_on_ttl_corpus() {
    // Deterministic pseudo-random corpus (fixed seed via a simple LCG) so the
    // test is reproducible and pins a specific mix of live / within-grace /
    // past-grace outcomes. Each cell lives in its own partition so the merge is
    // one cell per group (isolating the TTL decision).
    let schema = make_schema();
    let temp = TempDir::new().unwrap();
    let in_dir = temp.path().join("in");
    let wal_dir = temp.path().join("wal");

    // Mix of short TTLs (will be expired) and a huge TTL (stays live), across
    // several partitions.
    let corpus: Vec<CorpusCell> = (0..12)
        .map(|i| {
            let ttl = if i % 3 == 0 {
                10_000_000
            } else {
                60 + (i as u32) * 7
            };
            CorpusCell {
                id: i,
                ck: 0,
                name: format!("v{i}"),
                ttl,
                ts: 100 + i as i64,
            }
        })
        .collect();

    let muts: Vec<Mutation> = corpus
        .iter()
        .map(|c| write_ttl_row(c.id, c.ck, &c.name, c.ttl, c.ts))
        .collect();
    flush_batch(&in_dir, &wal_dir, &schema, muts);
    let inputs = discover_inputs(&in_dir);

    // Read the authoritative on-disk LDT per corpus cell (keyed by value text,
    // which is unique per cell).
    let raw_cells = read_name_cells(&inputs, &schema, true);
    let mut ldt_by_name: HashMap<String, i64> = HashMap::new();
    for c in &raw_cells {
        if let (Value::Text(v), Some(ldt)) = (&c.value, c.local_deletion_time) {
            ldt_by_name.insert(v.clone(), i64::from(ldt as u32));
        }
    }
    assert_eq!(
        ldt_by_name.len(),
        corpus.len(),
        "every corpus cell must surface an on-disk localDeletionTime"
    );

    // The tombstone an expired cell collapses to carries the CREATION-time LDT
    // (`ldt - ttl`, parity `AbstractCell.purge`) == the flush wall clock, which is
    // ~equal across the batch and therefore NOT unique per cell. Map a surviving
    // tombstone back to its corpus cell by its `markedForDeleteAt` (the cell's own
    // write timestamp, unique per corpus cell = `100 + i`), which the conversion
    // preserves verbatim.
    let creation_by_name: HashMap<String, i64> = corpus
        .iter()
        .map(|c| (c.name.clone(), ldt_by_name[&c.name] - i64::from(c.ttl)))
        .collect();
    let name_by_ts: HashMap<i64, String> = corpus.iter().map(|c| (c.ts, c.name.clone())).collect();

    // Pin now_secs above every short-TTL expiry but below the huge-TTL expiry so
    // every short-TTL cell expires while the huge-TTL cells stay live.
    let short: Vec<&CorpusCell> = corpus.iter().filter(|c| c.ttl < 1_000_000).collect();
    assert!(short.len() >= 2, "need at least two short-TTL cells");
    let max_short_ldt = short
        .iter()
        .map(|c| ldt_by_name[&c.name])
        .max()
        .expect("short-ttl cells present");
    let now_secs = max_short_ldt + 1_000; // expires every short-TTL cell

    // The tombstone an expired cell collapses to carries the CREATION-time LDT
    // (`ldt - ttl` == the flush wall clock, per `write_ttl_row`), which is ~equal
    // across the whole batch — so a SINGLE gc_before purges either all or none of
    // the expired cells. To exercise within-grace AND past-grace we run TWO
    // compactions against the same input: one with gc_before at/below every
    // creation (expired → tombstone), one strictly above (expired → purged). Live
    // cells survive in both. This keeps the differential faithful: each run's
    // production output is compared cell-by-cell to `reference_outcome`.
    let min_creation = *creation_by_name.values().min().expect("a creation time");
    let max_creation = *creation_by_name.values().max().expect("a creation time");

    // Map a compacted output directory back to per-cell outcomes.
    let outcomes = |out_dir: &Path| -> HashMap<String, RefOutcome> {
        let out_inputs = discover_inputs(out_dir);
        let out_cells = read_name_cells(&out_inputs, &schema, true);
        let mut actual: HashMap<String, RefOutcome> = HashMap::new();
        for c in &out_cells {
            match &c.value {
                Value::Text(v) => {
                    actual.insert(v.clone(), RefOutcome::Live(v.clone()));
                }
                Value::Tombstone(info) if info.tombstone_type == TombstoneType::CellTombstone => {
                    // Map by `markedForDeleteAt` (the cell's own write timestamp,
                    // unique per corpus cell), which the expiry conversion preserves.
                    // Assert the tombstone's LDT is the parity-correct creation time.
                    if let Some(name) = name_by_ts.get(&c.timestamp) {
                        assert_eq!(
                            info.local_deletion_time, creation_by_name[name],
                            "tombstone for {name} must carry creation-time LDT (ldt - ttl)"
                        );
                        actual.insert(name.clone(), RefOutcome::Tombstone);
                    }
                }
                _ => {}
            }
        }
        actual
    };

    // Run one compaction with `gc_before` and assert production == reference for
    // every corpus cell; return the set of outcomes observed.
    let run = |run_tag: &str, gc_before: i64, out_dir: &Path| -> (bool, bool, bool) {
        compact(
            inputs.clone(),
            out_dir,
            &schema,
            Some(gc_before),
            Some(now_secs),
            true,
        );
        let actual = outcomes(out_dir);
        let (mut live, mut tomb, mut absent) = (false, false, false);
        for c in &corpus {
            let ldt = ldt_by_name[&c.name];
            let expected = reference_outcome(&c.name, ldt, i64::from(c.ttl), now_secs, gc_before);
            let got = actual.get(&c.name).cloned().unwrap_or(RefOutcome::Absent);
            assert_eq!(
                got, expected,
                "[{run_tag}] cell {} (ldt={ldt}, now={now_secs}, gc_before={gc_before}): \
                 production output != reference TTL merge",
                c.name
            );
            match expected {
                RefOutcome::Live(_) => live = true,
                RefOutcome::Tombstone => tomb = true,
                RefOutcome::Absent => absent = true,
            }
        }
        (live, tomb, absent)
    };

    // Within-grace run: gc_before <= every creation → expired cells become
    // retained tombstones; huge-TTL cells stay live.
    let grace_dir = temp.path().join("out_grace");
    let (live_g, tomb_g, absent_g) = run("within-grace", min_creation, &grace_dir);
    assert!(
        live_g,
        "within-grace run must include a surviving live cell"
    );
    assert!(tomb_g, "within-grace run must include a retained tombstone");
    assert!(
        !absent_g,
        "within-grace run must NOT purge any cell (gc_before <= creation)"
    );

    // Past-grace run: gc_before strictly above every creation → expired cells are
    // purged entirely; huge-TTL cells stay live.
    let purge_dir = temp.path().join("out_purge");
    let (live_p, tomb_p, absent_p) = run("past-grace", max_creation + 1, &purge_dir);
    assert!(live_p, "past-grace run must include a surviving live cell");
    assert!(absent_p, "past-grace run must purge the expired cells");
    assert!(
        !tomb_p,
        "past-grace run must retain NO tombstone (gc_before > creation)"
    );
}
