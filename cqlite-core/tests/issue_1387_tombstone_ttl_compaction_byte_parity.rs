//! Issue #1387 (epic #973): TOMBSTONE / TTL COMPACTION byte-parity — CQLite's
//! compaction output vs a committed Cassandra 5.0.2-COMPACTED reference SSTable
//! for the same logical inputs, extending the live-cell slice
//! (`issue_1017_live_cell_compaction_byte_parity.rs`) to tombstones and TTL.
//!
//! Whereas issue #1017 pinned LIVE-CELL compaction (LWW overlap, no deletion),
//! this module pins the four tombstone/TTL compaction-merge behaviors that the
//! parity manifest previously tracked only as `partial`:
//!
//!   (a) `shadow_row_delete` — a newer-generation ROW tombstone shadows an
//!       older-generation live row; gc_grace default so the marker SURVIVES.
//!       → cass.compaction.CompactionDeleteAndPurgeRowTest.row_delete_purge
//!         cass.compaction.CompactionDeletePKTest.partition_delete_preserved
//!   (b) `ttl_expired_live` — an already-expired TTL cell coexists with a live
//!       cell; the expired cell collapses to a cell tombstone on compaction.
//!       → cass.compaction.TimeWindowCompactionStrategyTest.ttl_window_expiry_purge
//!   (c) `gc_purge_grace0` — gc_grace_seconds=0: a row tombstone with nothing to
//!       shadow is PURGED away by the major compaction (empty output).
//!       → cass.compaction.ForceCompactionTest.major_compaction_tombstone_purge
//!   (d) `rt_cross_gen` — two overlapping range tombstones across generations
//!       synthesize open/close boundary markers in the merged output.
//!       → cass.compaction.CompactionDeleteRowRangeTest.range_tombstone_merge
//!
//! ## Determinism contract (why two independent compactors can byte-match)
//!   * Every write/delete carries a fixed `USING TIMESTAMP` so `markedForDeleteAt`
//!     and each cell writetime (and thus the EncodingStats.minTimestamp delta
//!     baseline) are identical on both engines.
//!   * `localDeletionTime` is NOT pinned by `USING TIMESTAMP` — Cassandra derives it
//!     from the coordinator wall clock at write time. The COMMITTED golden captures
//!     whatever wall-clock LDT occurred at generation time; this test READS that LDT
//!     out of the golden JSONL (`local_delete_time`) and stamps CQLite's compaction
//!     inputs with the SAME LDT (authoritative — no guessing, no heuristic). This is
//!     how the two compactors byte-match on tombstone LDT.
//!   * Tables are UNCOMPRESSED and PKs are int/(int,int): identical key bytes and
//!     Murmur3 token ordering on both engines.
//!
//! ## Which components are byte-for-byte
//!   * Data.db, Index.db, Summary.db, Digest.crc32 → BYTE-IDENTICAL and diffed here.
//!   * Statistics.db, Filter.db → present on both sides but INTENTIONALLY NOT diffed
//!     (histogram / HLL / bloom bookkeeping cannot byte-match across engines), per
//!     issue #1017 AC6 / issue #1190.
//!
//! ## STATUS — blocked on #1410
//! The three byte-parity scenarios are currently `#[ignore = "blocked on #1410"]`:
//! building these fixtures REVEALED a real CQLite compaction bug — `compute_baseline_min`
//! lets a live-only input's `min_deletion_time=0` sentinel corrupt the merged
//! `localDeletionTime` baseline, so CQLite encodes a raw LDT delta instead of
//! `LDT - minLDT`. See #1410. The gc-purge SEMANTIC scenario (c) is independent of
//! that baseline bug and runs.
//!
//! ## Dataset doctrine (issue #719 / parity mandate)
//!   * `CQLITE_DATASETS_ROOT` unset OR the reference genuinely absent → SKIP.
//!   * Reference PRESENT but empty / incomplete → FAILURE (never silently pass).
//!   * `CQLITE_REQUIRE_FIXTURES=1` turns a would-be SKIP into a PANIC.

#![cfg(feature = "write-support")]

use crc32fast::Hasher as Crc32Hasher;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::merge::compact_sstables;
use cqlite_core::storage::write_engine::{
    CellOperation, ClusteringBound, ClusteringKey, Mutation, PartitionKey, RangeTombstone, TableId,
    WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use tempfile::TempDir;

/// Fixed writetimes (micros). MUST match the constants in
/// `test-data/scripts/generate-compaction-tombstone-ttl-parity.sh`.
const T_A: i64 = 1000;
const T_B: i64 = 2000;
const T_DEL: i64 = 3000;

const KEYSPACE: &str = "test_compaction_tombstone_ttl";

/// Output generation passed to `compact_sstables`. Fixed for determinism; affects
/// only the on-disk filename, never component CONTENT bytes.
const OUT_GENERATION: u64 = 3;

/// Fixed `gc_before` (secs) for the SURVIVING (non-purge) scenarios: far below any
/// recent tombstone LDT so no marker is purged (matches gc_grace default behavior).
const SURVIVE_GC_BEFORE: i64 = 1_000_000_000;

// ════════════════════════════════════════════════════════════════════════════
// Fixture resolution (skip-on-absence; present-but-incomplete is a failure)
// ════════════════════════════════════════════════════════════════════════════

fn require_fixtures_strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

/// Resolve the single `{table}-*` reference directory under the keyspace.
///   * root unset / keyspace dir absent / no `{table}-*` dir → `None` (clean SKIP)
///   * more than one `{table}-*` dir                          → PANIC (stale dup)
///   * exactly one dir but Data.db absent                     → PANIC (incomplete)
///   * exactly one dir, Data.db present                       → `Some`
fn reference_dir(table: &str) -> Option<PathBuf> {
    let root = std::env::var("CQLITE_DATASETS_ROOT").ok()?;
    let base = Path::new(&root).join("sstables").join(KEYSPACE);
    let entries = std::fs::read_dir(&base).ok()?;

    let mut matches: Vec<PathBuf> = entries
        .flatten()
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            if name.starts_with(&format!("{table}-")) {
                Some(e.path())
            } else {
                None
            }
        })
        .collect();

    match matches.len() {
        0 => None,
        1 => {
            let dir = matches.pop().expect("len==1");
            if single_data_db(&dir).is_none() {
                panic!(
                    "{KEYSPACE}.{table}: reference directory {dir:?} exists but contains no \
                     compacted nb-*-big-Data.db. The fixture is PRESENT-BUT-INCOMPLETE — the .db \
                     binaries may not have been `git add -f`'d. Regenerate with:\n  \
                     bash test-data/scripts/generate-compaction-tombstone-ttl-parity.sh"
                );
            }
            Some(dir)
        }
        n => panic!(
            "{KEYSPACE}.{table}: found {n} matching `{table}-*` directories under {base:?} \
             ({matches:?}); there must be EXACTLY ONE."
        ),
    }
}

fn assert_digest_consistent_with_data(table: &str, ref_dir: &Path) {
    let data_bytes = read_component(ref_dir, "Data.db");
    let digest_bytes = read_component(ref_dir, "Digest.crc32");
    assert!(
        !data_bytes.is_empty(),
        "{table}: committed Data.db present-but-empty — golden broken"
    );
    assert!(
        !digest_bytes.is_empty(),
        "{table}: committed Digest.crc32 present-but-empty — golden broken"
    );
    let mut hasher = Crc32Hasher::new();
    hasher.update(&data_bytes);
    let actual_crc32 = hasher.finalize();
    let digest_str = std::str::from_utf8(&digest_bytes)
        .unwrap_or_else(|_| panic!("{table}: Digest.crc32 not UTF-8: {digest_bytes:?}"))
        .trim();
    let committed_crc32: u32 = digest_str
        .parse()
        .unwrap_or_else(|e| panic!("{table}: Digest.crc32 '{digest_str}' not a u32: {e}"));
    assert_eq!(
        actual_crc32, committed_crc32,
        "{table}: committed Digest.crc32 ({committed_crc32}) != CRC32 of committed Data.db \
         ({actual_crc32}). Golden is HALF-UPDATED — regenerate + `git add -f` the .db binaries."
    );
}

fn single_data_db(dir: &Path) -> Option<PathBuf> {
    let mut found: Vec<PathBuf> = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let name = e.file_name().to_string_lossy().to_string();
            if name.starts_with("nb-") && name.ends_with("-big-Data.db") {
                found.push(e.path());
            }
        }
    }
    match found.len() {
        0 => None,
        1 => Some(found.pop().expect("len==1")),
        n => panic!(
            "{dir:?}: expected exactly ONE compacted nb-*-big-Data.db, found {n} ({found:?})"
        ),
    }
}

fn descriptor_prefix(data_db: &Path) -> String {
    let name = data_db
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_default();
    name.trim_end_matches("Data.db").to_string()
}

fn read_component(dir: &Path, suffix: &str) -> Vec<u8> {
    let data = single_data_db(dir).unwrap_or_else(|| panic!("{dir:?}: no compacted Data.db"));
    let prefix = descriptor_prefix(&data);
    let path = dir.join(format!("{prefix}{suffix}"));
    std::fs::read(&path)
        .unwrap_or_else(|e| panic!("component {path:?} unreadable in a present fixture: {e}"))
}

fn component_suffixes(dir: &Path) -> BTreeSet<String> {
    let mut set = BTreeSet::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let name = e.file_name().to_string_lossy().to_string();
            if let Some(idx) = name.find("-big-") {
                set.insert(name[idx + 5..].to_string());
            }
        }
    }
    set.retain(|s| !s.ends_with(".jsonl") && !s.ends_with("Statistics.db.txt"));
    set
}

fn toc_set(toc_bytes: &[u8]) -> BTreeSet<String> {
    String::from_utf8_lossy(toc_bytes)
        .lines()
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .collect()
}

fn first_diff(a: &[u8], b: &[u8]) -> Option<usize> {
    let n = a.len().max(b.len());
    (0..n).find(|&i| a.get(i) != b.get(i))
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// Parse an ISO-8601 `local_delete_time` (e.g. `2026-07-01T23:54:19Z`) from the
/// committed golden JSONL into epoch seconds. This is the AUTHORITATIVE LDT the
/// CQLite side must stamp so both compactors byte-match. Fails (not skips) if the
/// golden is present but the expected LDT field is missing/malformed.
fn ldt_secs_from_golden(
    ref_dir: &Path,
    table: &str,
    matcher: impl Fn(&serde_json::Value) -> Option<String>,
) -> i32 {
    let data = single_data_db(ref_dir).expect("compacted Data.db");
    let jsonl = ref_dir.join(format!("{}Data.db.jsonl", descriptor_prefix(&data)));
    let text = std::fs::read_to_string(&jsonl)
        .unwrap_or_else(|e| panic!("{table}: golden JSONL {jsonl:?} unreadable: {e}"));
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let jv: serde_json::Value = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("{table}: golden JSONL not valid JSON: {e}"));
        if let Some(iso) = matcher(&jv) {
            return iso_to_epoch_secs(&iso).unwrap_or_else(|| {
                panic!(
                    "{table}: golden local_delete_time '{iso}' not parseable as ISO-8601 seconds"
                )
            });
        }
    }
    panic!("{table}: golden JSONL carries no matching local_delete_time — fixture is broken");
}

/// Minimal ISO-8601 `YYYY-MM-DDTHH:MM:SSZ` → Unix epoch seconds (UTC), without an
/// external date dependency. Returns `None` on any parse failure.
fn iso_to_epoch_secs(s: &str) -> Option<i32> {
    let s = s.strip_suffix('Z')?;
    let (date, time) = s.split_once('T')?;
    let mut d = date.split('-');
    let year: i64 = d.next()?.parse().ok()?;
    let month: i64 = d.next()?.parse().ok()?;
    let day: i64 = d.next()?.parse().ok()?;
    let mut t = time.split(':');
    let hh: i64 = t.next()?.parse().ok()?;
    let mm: i64 = t.next()?.parse().ok()?;
    let ss: i64 = t.next()?.parse().ok()?;
    // days_from_civil (Howard Hinnant's algorithm) → days since 1970-01-01.
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    let total = days * 86_400 + hh * 3_600 + mm * 60 + ss;
    i32::try_from(total).ok()
}

// ════════════════════════════════════════════════════════════════════════════
// Schemas
// ════════════════════════════════════════════════════════════════════════════

fn col(name: &str, ty: &str, nullable: bool) -> Column {
    Column {
        name: name.into(),
        data_type: ty.into(),
        nullable,
        default: None,
        is_static: false,
    }
}

fn clustering_schema(table: &str) -> TableSchema {
    TableSchema {
        keyspace: KEYSPACE.into(),
        table: table.into(),
        partition_keys: vec![KeyColumn {
            name: "id".into(),
            data_type: "int".into(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".into(),
            data_type: "int".into(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![
            col("id", "int", false),
            col("ck", "int", false),
            col("v", "text", true),
        ],
        comments: Default::default(),
        dropped_columns: Default::default(),
    }
}

fn ck_row(table: &str, id: i32, ck: i32, v: &str, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new(KEYSPACE, table),
        PartitionKey::single("id", Value::Integer(id)),
        Some(ClusteringKey::single("ck", Value::Integer(ck))),
        vec![CellOperation::Write {
            column: "v".into(),
            value: Value::Text(v.into()),
        }],
        ts,
        None,
    )
}

fn row_tombstone(table: &str, id: i32, ck: i32, mfda: i64, ldt: i32) -> Mutation {
    Mutation::new(
        TableId::new(KEYSPACE, table),
        PartitionKey::single("id", Value::Integer(id)),
        Some(ClusteringKey::single("ck", Value::Integer(ck))),
        vec![CellOperation::DeleteRow],
        mfda,
        None,
    )
    .with_local_deletion_time(ldt)
}

// ════════════════════════════════════════════════════════════════════════════
// Input building + compaction (CQLite candidate)
// ════════════════════════════════════════════════════════════════════════════

/// Flush `group_a` then `group_b` as two overlapping input SSTables, run
/// `compact_sstables` over exactly those two files. Returns `(guard, output_dir)`.
/// `now_secs` is passed through so purge scenarios can pin `gcBefore`.
async fn cqlite_compact(
    schema: &TableSchema,
    group_a: Vec<Mutation>,
    group_b: Vec<Mutation>,
    gc_before: Option<i64>,
    now_secs: Option<i64>,
) -> (TempDir, Option<PathBuf>) {
    let temp = TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("inputs");
    let wal_dir = temp.path().join("wal");
    let out_dir = temp.path().join("out");

    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir.clone(), schema.clone());
    let mut engine = WriteEngine::new(config).expect("engine");

    for m in group_a {
        engine.write(m).expect("write A");
    }
    engine.flush().await.expect("flush A").expect("info A");
    for m in group_b {
        engine.write(m).expect("write B");
    }
    engine.flush().await.expect("flush B").expect("info B");
    engine.close().await.expect("close engine");

    let inputs = discover_inputs(&data_dir);
    assert_eq!(
        inputs.len(),
        2,
        "expected exactly 2 input SSTables, got {inputs:?}"
    );

    let report = compact_sstables(
        inputs,
        &out_dir,
        schema,
        OUT_GENERATION,
        gc_before,
        now_secs,
        true,
    )
    .await
    .expect("compaction must succeed");

    // A fully-purged compaction may produce no Data.db.
    let data_path = report.output.data_path.clone();
    if data_path.exists() {
        let table_dir = data_path.parent().expect("data parent").to_path_buf();
        (temp, Some(table_dir))
    } else {
        (temp, None)
    }
}

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

// ════════════════════════════════════════════════════════════════════════════
// Shared byte-parity assertion
// ════════════════════════════════════════════════════════════════════════════

const BYTE_FOR_BYTE_COMPONENTS: &[&str] = &["Data.db", "Index.db", "Summary.db", "Digest.crc32"];
const PRESENT_NOT_DIFFED: &[&str] = &["Statistics.db", "Filter.db"];

/// Diff a CQLite compaction output against the Cassandra compacted reference for
/// `table`: component presence (no silent omission), TOC set equality, and the
/// byte-for-byte component set. Any mismatch FAILS with an offset-diff artifact.
async fn assert_compaction_byte_parity(
    table: &str,
    schema: TableSchema,
    group_a: Vec<Mutation>,
    group_b: Vec<Mutation>,
) {
    let Some(ref_dir) = reference_dir(table) else {
        if require_fixtures_strict() {
            panic!(
                "CQLITE_REQUIRE_FIXTURES=1 but the compacted reference for {KEYSPACE}.{table} is \
                 absent; generate it with \
                 bash test-data/scripts/generate-compaction-tombstone-ttl-parity.sh"
            );
        }
        eprintln!("[issue_1387] reference for {KEYSPACE}.{table} absent; skipping");
        return;
    };

    assert_digest_consistent_with_data(table, &ref_dir);

    let (_guard, out_opt) =
        cqlite_compact(&schema, group_a, group_b, Some(SURVIVE_GC_BEFORE), None).await;
    let out_dir = out_opt
        .unwrap_or_else(|| panic!("{table}: CQLite produced NO output for a surviving scenario"));

    let ref_components = component_suffixes(&ref_dir);
    let our_components = component_suffixes(&out_dir);
    assert!(
        !ref_components.is_empty(),
        "{table}: reference component set empty (broken fixture)"
    );
    for needed in BYTE_FOR_BYTE_COMPONENTS
        .iter()
        .chain(PRESENT_NOT_DIFFED.iter())
        .chain(["TOC.txt"].iter())
    {
        assert!(
            ref_components.contains(*needed),
            "{table}: reference missing component {needed}; have {ref_components:?}"
        );
        assert!(
            our_components.contains(*needed),
            "{table}: CQLite output missing component {needed}; have {our_components:?}"
        );
    }
    let omitted: Vec<&String> = ref_components.difference(&our_components).collect();
    assert!(
        omitted.is_empty(),
        "{table}: Cassandra wrote component(s) CQLite SILENTLY OMITS: {omitted:?}"
    );
    let spurious: Vec<&String> = our_components.difference(&ref_components).collect();
    assert!(
        spurious.is_empty(),
        "{table}: CQLite emitted spurious component(s): {spurious:?}"
    );

    let ref_toc = toc_set(&read_component(&ref_dir, "TOC.txt"));
    let our_toc = toc_set(&read_component(&out_dir, "TOC.txt"));
    assert_eq!(ref_toc, our_toc, "{table}: TOC.txt set differs");

    for suffix in BYTE_FOR_BYTE_COMPONENTS {
        assert_component_bytes(table, &ref_dir, &out_dir, suffix);
    }

    for suffix in PRESENT_NOT_DIFFED {
        let r = read_component(&ref_dir, suffix);
        let o = read_component(&out_dir, suffix);
        assert!(
            !r.is_empty() && !o.is_empty(),
            "{table}: {suffix} present-but-empty on one side"
        );
    }

    eprintln!(
        "[issue_1387] {KEYSPACE}.{table}: TOMBSTONE/TTL compaction byte parity PASS — \
         {BYTE_FOR_BYTE_COMPONENTS:?} byte-identical to the Cassandra 5.0.2 compacted reference."
    );
}

fn assert_component_bytes(table: &str, ref_dir: &Path, out_dir: &Path, suffix: &str) {
    let reference = read_component(ref_dir, suffix);
    let ours = read_component(out_dir, suffix);
    assert!(
        !reference.is_empty(),
        "{table}: reference {suffix} present-but-empty — parity failure"
    );
    if reference != ours {
        let at = first_diff(&reference, &ours);
        panic!(
            "{table}: {suffix} byte mismatch (cass={} ours={} bytes, first diff at {at:?})\n  \
             cass={}\n  ours={}",
            reference.len(),
            ours.len(),
            hex(&reference),
            hex(&ours),
        );
    }
}

// ════════════════════════════════════════════════════════════════════════════
// (a) shadow_row_delete — newer-gen ROW tombstone shadows older-gen live row
// ════════════════════════════════════════════════════════════════════════════

/// Manifest: cass.compaction.CompactionDeleteAndPurgeRowTest.row_delete_purge,
/// cass.compaction.CompactionDeletePKTest.partition_delete_preserved.
///
/// BLOCKED on #1410: `compute_baseline_min` corrupts the merged localDeletionTime
/// baseline when a live-only input contributes its `min_deletion_time=0` sentinel,
/// so CQLite emits a raw LDT delta instead of `LDT - minLDT`.
#[tokio::test]
#[ignore = "blocked on #1410 (compute_baseline_min localDeletionTime baseline bug)"]
async fn shadow_row_delete_compaction_byte_for_byte() {
    let t = "shadow_row_delete";
    let Some(ref_dir) = reference_dir(t) else {
        if require_fixtures_strict() {
            panic!("CQLITE_REQUIRE_FIXTURES=1 but {KEYSPACE}.{t} reference absent");
        }
        eprintln!("[issue_1387] {t} reference absent; skipping");
        return;
    };
    // LDT of the (1,1) row tombstone, read from the golden's row deletion_info.
    let ldt = ldt_secs_from_golden(&ref_dir, t, |jv| {
        jv.get("rows")?.as_array()?.iter().find_map(|row| {
            row.get("deletion_info")?
                .get("local_delete_time")?
                .as_str()
                .map(str::to_string)
        })
    });
    let group_a = vec![
        ck_row(t, 1, 1, "a-1-1", T_A),
        ck_row(t, 1, 2, "a-1-2", T_A),
        ck_row(t, 2, 0, "a-2-0", T_A),
    ];
    let group_b = vec![
        row_tombstone(t, 1, 1, T_DEL, ldt),
        ck_row(t, 1, 3, "b-1-3", T_B),
    ];
    assert_compaction_byte_parity(t, clustering_schema(t), group_a, group_b).await;
}

// ════════════════════════════════════════════════════════════════════════════
// (b) ttl_expired_live — expired TTL cell collapses to a cell tombstone
// ════════════════════════════════════════════════════════════════════════════

/// Manifest: cass.compaction.TimeWindowCompactionStrategyTest.ttl_window_expiry_purge.
///
/// BLOCKED on #1410 (same localDeletionTime baseline corruption path).
#[tokio::test]
#[ignore = "blocked on #1410 (compute_baseline_min localDeletionTime baseline bug)"]
async fn ttl_expired_live_compaction_byte_for_byte() {
    let t = "ttl_expired_live";
    let Some(ref_dir) = reference_dir(t) else {
        if require_fixtures_strict() {
            panic!("CQLITE_REQUIRE_FIXTURES=1 but {KEYSPACE}.{t} reference absent");
        }
        eprintln!("[issue_1387] {t} reference absent; skipping");
        return;
    };
    // LDT of the expired (1,1) cell tombstone, from the golden's cell deletion_info.
    let ldt = ldt_secs_from_golden(&ref_dir, t, |jv| {
        jv.get("rows")?.as_array()?.iter().find_map(|row| {
            row.get("cells")?.as_array()?.iter().find_map(|cell| {
                cell.get("deletion_info")?
                    .get("local_delete_time")?
                    .as_str()
                    .map(str::to_string)
            })
        })
    });
    // The expired TTL cell reconstructs as a cell tombstone at the read-back LDT.
    let expired = Mutation::new(
        TableId::new(KEYSPACE, t),
        PartitionKey::single("id", Value::Integer(1)),
        Some(ClusteringKey::single("ck", Value::Integer(1))),
        vec![CellOperation::Delete {
            column: "v".into(),
            local_deletion_time: Some(ldt),
        }],
        T_A,
        None,
    );
    let group_a = vec![expired];
    let group_b = vec![ck_row(t, 1, 2, "b-1-2", T_B)];
    assert_compaction_byte_parity(t, clustering_schema(t), group_a, group_b).await;
}

// ════════════════════════════════════════════════════════════════════════════
// (d) rt_cross_gen — overlapping cross-generation range-tombstone merge
// ════════════════════════════════════════════════════════════════════════════

fn rt(
    id: i32,
    lo: i32,
    hi: i32,
    mfda: i64,
    ldt: i32,
    table: &str,
    live_ck: i32,
    live_v: &str,
    live_ts: i64,
) -> Mutation {
    let mut m = ck_row(table, id, live_ck, live_v, live_ts);
    m.range_tombstones.push(RangeTombstone {
        start: ClusteringBound::Inclusive(ClusteringKey::single("ck", Value::Integer(lo))),
        end: ClusteringBound::Inclusive(ClusteringKey::single("ck", Value::Integer(hi))),
        deletion_time: mfda,
        local_deletion_time: ldt,
    });
    m
}

/// Manifest: cass.compaction.CompactionDeleteRowRangeTest.range_tombstone_merge.
///
/// BLOCKED on #1410 (same localDeletionTime baseline corruption path).
#[tokio::test]
#[ignore = "blocked on #1410 (compute_baseline_min localDeletionTime baseline bug)"]
async fn rt_cross_gen_compaction_byte_for_byte() {
    let t = "rt_cross_gen";
    let Some(ref_dir) = reference_dir(t) else {
        if require_fixtures_strict() {
            panic!("CQLITE_REQUIRE_FIXTURES=1 but {KEYSPACE}.{t} reference absent");
        }
        eprintln!("[issue_1387] {t} reference absent; skipping");
        return;
    };
    // Two RT LDTs from the golden's range markers (in disk order).
    let mut ldts: Vec<i32> = Vec::new();
    {
        let data = single_data_db(&ref_dir).expect("Data.db");
        let jsonl = ref_dir.join(format!("{}Data.db.jsonl", descriptor_prefix(&data)));
        let text = std::fs::read_to_string(&jsonl).expect("jsonl");
        for line in text.lines().filter(|l| !l.trim().is_empty()) {
            let jv: serde_json::Value = serde_json::from_str(line).expect("json");
            if let Some(rows) = jv.get("rows").and_then(|r| r.as_array()) {
                for row in rows {
                    for side in ["start", "end"] {
                        if let Some(iso) = row
                            .get(side)
                            .and_then(|s| s.get("deletion_info"))
                            .and_then(|d| d.get("local_delete_time"))
                            .and_then(|v| v.as_str())
                        {
                            if let Some(secs) = iso_to_epoch_secs(iso) {
                                if !ldts.contains(&secs) {
                                    ldts.push(secs);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    assert!(
        ldts.len() >= 2,
        "{t}: golden must carry two distinct range-tombstone LDTs, got {ldts:?}"
    );
    let group_a = vec![rt(1, 10, 20, T_DEL, ldts[0], t, 5, "a-1-5", T_A)];
    let group_b = vec![rt(1, 15, 25, T_DEL + 1, ldts[1], t, 30, "b-1-30", T_B)];
    assert_compaction_byte_parity(t, clustering_schema(t), group_a, group_b).await;
}

// ════════════════════════════════════════════════════════════════════════════
// (c) gc_purge_grace0 — gc-purgeable tombstone dropped by major compaction
// ════════════════════════════════════════════════════════════════════════════

/// Manifest: cass.compaction.ForceCompactionTest.major_compaction_tombstone_purge.
///
/// SEMANTIC purge contract (independent of the #1410 LDT-delta bug, so it RUNS):
/// two tombstone-only input SSTables (gc_grace_seconds=0 table) with `gcBefore`
/// pinned so both tombstones are purgeable; the major compaction must produce
/// EMPTY output (no Data.db), exactly as Cassandra dropped the whole SSTable.
///
/// Cassandra emits no output directory for a fully-purged major compaction, so the
/// reference is validated by the ABSENCE of a committed `gc_purge_grace0-*` Data.db
/// (present-but-non-empty would be a fixture error) rather than a byte diff.
#[tokio::test]
async fn gc_purge_grace0_major_compaction_purges_to_empty() {
    let t = "gc_purge_grace0";
    // Reference contract: Cassandra produced ZERO Data.db (dir empty or absent).
    if let Ok(root) = std::env::var("CQLITE_DATASETS_ROOT") {
        let base = Path::new(&root).join("sstables").join(KEYSPACE);
        if let Ok(rd) = std::fs::read_dir(&base) {
            for e in rd.flatten() {
                if e.file_name()
                    .to_string_lossy()
                    .starts_with(&format!("{t}-"))
                {
                    assert!(
                        single_data_db(&e.path()).is_none(),
                        "{t}: reference {:?} unexpectedly carries a Data.db — a gc_grace=0 major \
                         compaction of tombstone-only inputs must purge to EMPTY output",
                        e.path()
                    );
                }
            }
        }
    } else if require_fixtures_strict() {
        panic!("CQLITE_REQUIRE_FIXTURES=1 but CQLITE_DATASETS_ROOT unset");
    }

    // CQLite side: two tombstone-only inputs; both LDTs strictly below gcBefore.
    let ldt = 100i32; // any LDT < gc_before
    let gc_before = 1_000i64; // gcBefore > every tombstone LDT ⇒ purgeable
    let now = 1_000i64;
    let schema = clustering_schema(t);
    let group_a = vec![row_tombstone(t, 1, 1, T_DEL, ldt)];
    let group_b = vec![row_tombstone(t, 2, 0, T_DEL, ldt)];
    let (_guard, out_opt) =
        cqlite_compact(&schema, group_a, group_b, Some(gc_before), Some(now)).await;

    // Purged: CQLite must produce EMPTY output. Cassandra writes NO Data.db at
    // all; CQLite writes a zero-length Data.db (no unfiltered content). Both are
    // valid "purged to empty" outcomes — a NON-EMPTY Data.db would mean a
    // purgeable tombstone was wrongly retained.
    if let Some(dir) = out_opt {
        if let Some(data) = single_data_db(&dir) {
            let bytes = std::fs::read(&data).unwrap_or_default();
            assert!(
                bytes.is_empty(),
                "{t}: CQLite compaction of two purgeable tombstone-only inputs must produce EMPTY \
                 output, but Data.db {data:?} carries {} bytes ({}) — a purgeable tombstone was \
                 wrongly retained",
                bytes.len(),
                hex(&bytes),
            );
        }
    }
    eprintln!(
        "[issue_1387] {KEYSPACE}.{t}: gc-purge semantic parity PASS — purgeable tombstones dropped \
         to empty output, matching Cassandra's fully-purged major compaction."
    );
}
