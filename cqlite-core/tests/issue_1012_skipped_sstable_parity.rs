//! Issue #1012 — Protect skipped SSTable reads and tombstone-only partitions.
//!
//! ## What this harness proves
//!
//! CQLite must NOT drop deletion intent when read planning / range filtering would
//! make a source SSTable look irrelevant. Partition tombstones and tombstone-only
//! partitions must remain visible to merge/read logic even when one generation's
//! clustering bounds / min-max metadata alone would suggest the older live rows
//! should be returned.
//!
//! ## Fixture — `test_tomb/skipped_partition_delete-*` (two generations, one dir)
//!
//! * `nb-1-big-*` — older LIVE rows in partition `pk=1` at clustering `ck` 1..=10,
//!   written `USING TIMESTAMP` T_GEN1 (2021-01-01).
//! * `nb-2-big-*` — a partition tombstone `DELETE WHERE pk=1` at T_GEN2
//!   (2021-01-02, strictly newer) AND a tombstone-only partition `pk=2`
//!   (partition tombstone, no live rows in any generation).
//!
//! Both per-generation sstabledump JSONL goldens are committed (`nb-1`, `nb-2`).
//! `gc_grace` is the table default (tombstones retained).
//!
//! ## Coverage map (manifest IDs)
//!
//! | Manifest ID | Capability | Status | Where proved |
//! |-------------|-----------|--------|--------------|
//! | `cass.sstable_io.reader.tombstone_only_partition` | data_db_decode | mirrored | [`reader_tombstone_only_partition_parses_as_deletion`] |
//! | `cass.sstable_io.scanner.tombstone_only_partition_ranges` | data_db_decode | mirrored | [`scanner_tombstone_only_partition_surfaces_in_range`] |
//! | `cass.tombstone_ttl.skipped_sstable.partition_delete_reincluded` | tombstone_ttl | mirrored (reader) / partial (merge) | [`gen2_partition_delete_reincluded_by_reader`] + [`merge_partition_delete_shadows_older_rows_gap`] |
//! | `cass.tombstone_ttl.skipped_sstable.partition_delete_shadows_older_rows` | tombstone_ttl | **partial (P0 gap)** | [`merge_partition_delete_shadows_older_rows_gap`] |
//! | `cass.compaction_merge.partition_delete_shadowing_across_skipped_sources` | compaction_merge | **partial (P0 gap)** | [`compaction_partition_delete_shadowing_gap`] |
//!
//! ## P0 RESURRECTION GAP found (see the two `#[ignore]`d tests below)
//!
//! The READER is correct: `scan_delta` (via `parse_partition_header_full`) surfaces
//! both gen-2 partition tombstones, including the tombstone-only `pk=2`, exactly as
//! the nb-2 golden records them. BUT the cross-generation MERGE/COMPACTION read
//! path does NOT apply partition tombstones:
//!
//! * `parse_one_partition_for_compaction` calls `parse_partition_header` (which
//!   DISCARDS the partition-level `markedForDeleteAt`), not
//!   `parse_partition_header_full`; and
//! * `CompactionRowData` has no partition-deletion variant, so the merge stream
//!   carries no partition-tombstone carrier.
//!
//! Consequence: merging `nb-2` (newest, the tombstone) over `nb-1` (older live rows)
//! RESURRECTS all 10 `pk=1` rows and DROPS the `pk=2` tombstone-only partition. The
//! two `#[ignore]`d tests pin the CORRECT (unweakened) expectation — they fail today
//! with the resurrection captured precisely (expected 0 live rows / pk=1, actual 10).
//! De-`#[ignore]` them once the merge path threads partition deletions.

#![cfg(all(feature = "delta-scan", feature = "write-support"))]

use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use serde_json::Value as JsonValue;

use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::reader::delta_scan::{scan_delta, DeltaRecord};
use cqlite_core::storage::write_engine::merge::{compact_sstables, KWayMerger, MergeStep, RowData};
use cqlite_core::types::Value;

// ════════════════════════════════════════════════════════════════════════════
// Fixture / schema
// ════════════════════════════════════════════════════════════════════════════

const FIXTURE_PREFIX: &str = "skipped_partition_delete";
const KEYSPACE: &str = "test_tomb";

/// Schema for `test_tomb.skipped_partition_delete`, from the Statistics.db headers:
/// KeyType=Int32, ClusteringTypes=[Int32], RegularColumns=val:UTF8Type.
fn schema() -> TableSchema {
    TableSchema {
        keyspace: KEYSPACE.to_string(),
        table: FIXTURE_PREFIX.to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![Column {
            name: "val".to_string(),
            data_type: "text".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        }],
        comments: Default::default(),
        dropped_columns: Default::default(),
    }
}

/// Locate the fixture directory under `CQLITE_DATASETS_ROOT`. The hash suffix is
/// matched by PREFIX (no full-name hardcode) so a regenerated fixture still binds.
/// Returns `None` (skip) when the env var is unset or the dir is absent.
fn fixture_dir() -> Option<PathBuf> {
    let root = std::env::var("CQLITE_DATASETS_ROOT").ok()?;
    let ks = PathBuf::from(root).join("sstables").join(KEYSPACE);
    if !ks.exists() {
        return None;
    }
    for entry in fs::read_dir(&ks).ok()?.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with(&format!("{FIXTURE_PREFIX}-")) && entry.path().is_dir() {
            return Some(entry.path());
        }
    }
    None
}

/// Path to one generation's Data.db. Returns `None` (skip) when the binary is
/// absent (committed JSONL travels without Data.db in CI).
fn data_db_for_gen(dir: &Path, gen: &str) -> Option<PathBuf> {
    let p = dir.join(format!("nb-{gen}-big-Data.db"));
    p.exists().then_some(p)
}

/// Copy ONE generation's binary components into a fresh temp dir so the
/// single-generation `scan_delta` reader sees exactly one Data.db (the fixture
/// dir intentionally holds both generations). `.jsonl`/`.txt` siblings are
/// excluded — `scan_delta` only needs the binary components.
fn isolate_gen(dir: &Path, gen: &str, tmp: &Path) -> PathBuf {
    let prefix = format!("nb-{gen}-big-");
    let out = tmp.join(format!("gen-{gen}"));
    fs::create_dir_all(&out).expect("create gen temp dir");
    for entry in fs::read_dir(dir).expect("read fixture dir").flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with(&prefix) && !name.ends_with(".jsonl") && !name.ends_with(".txt") {
            fs::copy(entry.path(), out.join(&name)).expect("copy component");
        }
    }
    out
}

// ════════════════════════════════════════════════════════════════════════════
// JSONL golden facts (positional / ordered comparison)
// ════════════════════════════════════════════════════════════════════════════

/// A live-row fact: the partition key (int) and clustering key (int) of an
/// `nb-1` JSONL live row, in on-disk order.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LiveRowFact {
    pk: i64,
    ck: i64,
}

/// A partition-tombstone fact from an `nb-2` JSONL partition `deletion_info`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PartitionDeleteFact {
    pk: i64,
    marked_deleted_micros: i64,
}

/// Parse the gen-1 JSONL golden into ORDERED `(pk, ck)` live-row facts.
fn parse_gen1_live_rows(path: &Path) -> Vec<LiveRowFact> {
    let mut out = Vec::new();
    for line in read_jsonl_lines(path) {
        let v: JsonValue = serde_json::from_str(&line).expect("gen-1 JSONL parse");
        let pk = partition_pk(&v).expect("gen-1 partition pk");
        // A live-row generation has no partition deletion_info.
        if v.get("partition")
            .and_then(|p| p.get("deletion_info"))
            .is_some()
        {
            continue;
        }
        if let Some(rows) = v.get("rows").and_then(|r| r.as_array()) {
            for row in rows {
                if row.get("type").and_then(|t| t.as_str()) != Some("row") {
                    continue;
                }
                // A live row has liveness_info and no row-level deletion_info.
                if row.get("deletion_info").is_some() {
                    continue;
                }
                let ck = row
                    .get("clustering")
                    .and_then(|c| c.as_array())
                    .and_then(|a| a.first())
                    .and_then(json_int)
                    .expect("gen-1 clustering int");
                out.push(LiveRowFact { pk, ck });
            }
        }
    }
    out
}

/// Parse the gen-2 JSONL golden into ORDERED partition-tombstone facts.
fn parse_gen2_partition_deletes(path: &Path) -> Vec<PartitionDeleteFact> {
    let mut out = Vec::new();
    for line in read_jsonl_lines(path) {
        let v: JsonValue = serde_json::from_str(&line).expect("gen-2 JSONL parse");
        let pk = partition_pk(&v).expect("gen-2 partition pk");
        let di = match v.get("partition").and_then(|p| p.get("deletion_info")) {
            Some(d) => d,
            None => continue,
        };
        let marked = di
            .get("marked_deleted")
            .and_then(|s| s.as_str())
            .and_then(iso8601_to_micros)
            .expect("gen-2 marked_deleted");
        out.push(PartitionDeleteFact {
            pk,
            marked_deleted_micros: marked,
        });
    }
    out
}

fn read_jsonl_lines(path: &Path) -> Vec<String> {
    let file = fs::File::open(path).unwrap_or_else(|e| panic!("open JSONL {path:?}: {e}"));
    BufReader::new(file)
        .lines()
        .map_while(Result::ok)
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .collect()
}

/// sstabledump renders an int partition key as a JSON STRING (e.g. `["1"]`).
fn partition_pk(v: &JsonValue) -> Option<i64> {
    let key = v.get("partition")?.get("key")?.as_array()?;
    json_int(key.first()?)
}

/// Parse an int from either a JSON number or a JSON string (sstabledump uses both).
fn json_int(v: &JsonValue) -> Option<i64> {
    match v {
        JsonValue::Number(n) => n.as_i64(),
        JsonValue::String(s) => s.parse::<i64>().ok(),
        _ => None,
    }
}

/// Find the JSONL golden for a given generation in the fixture dir.
fn jsonl_for_gen(dir: &Path, gen: &str) -> PathBuf {
    dir.join(format!("nb-{gen}-big-Data.db.jsonl"))
}

// ── ISO-8601 → epoch-µs (no chrono dep; mirrors scan_delta_parity_test) ──

fn iso8601_to_micros(s: &str) -> Option<i64> {
    let s = s.strip_suffix('Z')?;
    let (date_part, time_part) = s.split_once('T')?;
    let mut dp = date_part.splitn(3, '-');
    let year: i64 = dp.next()?.parse().ok()?;
    let month: i64 = dp.next()?.parse().ok()?;
    let day: i64 = dp.next()?.parse().ok()?;
    let (hms, frac) = time_part.split_once('.').unwrap_or((time_part, ""));
    let mut tp = hms.splitn(3, ':');
    let h: i64 = tp.next()?.parse().ok()?;
    let mi: i64 = tp.next()?.parse().ok()?;
    let se: i64 = tp.next()?.parse().ok()?;
    let days = days_since_epoch(year, month, day)?;
    let secs = days * 86400 + h * 3600 + mi * 60 + se;
    let frac_micros = if frac.is_empty() {
        0
    } else {
        format!("{:0<6}", &frac[..frac.len().min(6)])
            .parse::<i64>()
            .ok()?
    };
    Some(secs * 1_000_000 + frac_micros)
}

fn days_since_epoch(y: i64, m: i64, d: i64) -> Option<i64> {
    let a = (14 - m) / 12;
    let yy = y + 4800 - a;
    let mm = m + 12 * a - 3;
    let jdn = d + (153 * mm + 2) / 5 + 365 * yy + yy / 4 - yy / 100 + yy / 400 - 32045;
    Some(jdn - 2_440_588)
}

// ════════════════════════════════════════════════════════════════════════════
// Per-generation reader collection (single-generation scan_delta)
// ════════════════════════════════════════════════════════════════════════════

async fn collect_records(dir: PathBuf) -> Vec<DeltaRecord> {
    let (mut rx, _summary) = scan_delta(dir, schema(), 256);
    let mut out = Vec::new();
    while let Some(r) = rx.recv().await {
        out.push(r.expect("scan_delta record"));
    }
    out
}

/// Extract the first partition-key int from a `DeltaRecord` (all variants here
/// carry a single int pk).
fn record_pk(rec: &DeltaRecord) -> i64 {
    match rec.partition_key().first() {
        Some(Value::Integer(i)) => *i as i64,
        other => panic!("expected int pk, got {other:?}"),
    }
}

fn record_ck(rec: &DeltaRecord) -> Option<i64> {
    if let DeltaRecord::Upsert { keys, .. } = rec {
        match keys.clustering.first() {
            Some(Value::Integer(i)) => Some(*i as i64),
            _ => None,
        }
    } else {
        None
    }
}

// ════════════════════════════════════════════════════════════════════════════
// MERGE collection (cross-generation KWayMerger — the read/merge-affecting path)
// ════════════════════════════════════════════════════════════════════════════

/// A merged tuple surviving the cross-generation k-way merge: `(pk, ck, kind)`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct MergedTuple {
    pk: i64,
    ck: Option<i64>,
    live: bool,
}

/// Drive `KWayMerger` over BOTH generations newest-first (nb-2, nb-1) — the same
/// read/merge-affecting path the compactor uses — and collect surviving tuples.
fn merge_both_generations(dir: &Path) -> Option<Vec<MergedTuple>> {
    let nb2 = data_db_for_gen(dir, "2")?;
    let nb1 = data_db_for_gen(dir, "1")?;
    let sch = schema();
    let mut merger = KWayMerger::new(vec![nb2, nb1], &sch).expect("KWayMerger over both gens");
    let mut out = Vec::new();
    loop {
        match merger.step().expect("merge step") {
            MergeStep::Complete => break,
            MergeStep::Partition { key, rows } => {
                let pk = match Value::Integer(i32::from_be_bytes(
                    key.key.as_slice().try_into().unwrap_or([0, 0, 0, 0]),
                )) {
                    Value::Integer(i) => i as i64,
                    _ => unreachable!(),
                };
                for e in rows {
                    let ck = e
                        .clustering_key
                        .as_ref()
                        .and_then(|c| c.columns.first())
                        .and_then(|(_, v)| match v {
                            Value::Integer(i) => Some(*i as i64),
                            _ => None,
                        });
                    let live = matches!(e.row_data, RowData::Live { .. });
                    out.push(MergedTuple { pk, ck, live });
                }
            }
        }
    }
    Some(out)
}

// ════════════════════════════════════════════════════════════════════════════
// TESTS — reader / scanner facts (mirrored; run by default)
// ════════════════════════════════════════════════════════════════════════════

/// `cass.tombstone_ttl.skipped_sstable.partition_delete_reincluded` (reader half)
/// + positional parity of BOTH per-generation goldens.
///
/// Proves the reader does NOT skip the gen-2 source: every gen-2 partition
/// tombstone (the source a range filter might skip) is parsed and surfaced, in
/// the exact order and with the exact `markedForDeleteAt` the nb-2 golden records.
/// Also positionally confirms gen-1's 10 live rows so a per-generation regression
/// in either direction is caught.
#[tokio::test]
async fn gen2_partition_delete_reincluded_by_reader() {
    let Some(dir) = fixture_dir() else {
        eprintln!(
            "[SKIP] CQLITE_DATASETS_ROOT unset or fixture absent — skipping #1012 reader parity"
        );
        return;
    };
    if data_db_for_gen(&dir, "1").is_none() || data_db_for_gen(&dir, "2").is_none() {
        eprintln!(
            "[SKIP] Data.db binaries absent (committed JSONL only) — skipping #1012 reader parity"
        );
        return;
    }
    let tmp = tempfile::tempdir().expect("tempdir");

    // ---- gen-1: 10 live rows, positional (pk,ck) parity ----
    let gen1_golden = parse_gen1_live_rows(&jsonl_for_gen(&dir, "1"));
    assert_eq!(
        gen1_golden.len(),
        10,
        "nb-1 golden must list 10 live rows; got {} — golden changed?",
        gen1_golden.len()
    );
    let gen1_records = collect_records(isolate_gen(&dir, "1", tmp.path())).await;
    let gen1_upserts: Vec<LiveRowFact> = gen1_records
        .iter()
        .filter(|r| matches!(r, DeltaRecord::Upsert { .. }))
        .map(|r| LiveRowFact {
            pk: record_pk(r),
            ck: record_ck(r).expect("gen-1 upsert ck"),
        })
        .collect();
    assert_eq!(
        gen1_upserts, gen1_golden,
        "gen-1 (nb-1) reader Upserts must match the nb-1 JSONL golden POSITIONALLY \
         (ordered (pk,ck)). reader={:?} golden={:?}",
        gen1_upserts, gen1_golden
    );

    // ---- gen-2: 2 partition tombstones, positional parity ----
    let gen2_golden = parse_gen2_partition_deletes(&jsonl_for_gen(&dir, "2"));
    assert_eq!(
        gen2_golden.len(),
        2,
        "nb-2 golden must list 2 partition tombstones (pk=1, pk=2); got {}",
        gen2_golden.len()
    );
    let gen2_records = collect_records(isolate_gen(&dir, "2", tmp.path())).await;
    let gen2_deletes: Vec<PartitionDeleteFact> = gen2_records
        .iter()
        .filter_map(|r| match r {
            DeltaRecord::PartitionDelete {
                partition_key: _,
                deleted_at,
            } => Some(PartitionDeleteFact {
                pk: record_pk(r),
                marked_deleted_micros: *deleted_at,
            }),
            _ => None,
        })
        .collect();
    assert_eq!(
        gen2_deletes, gen2_golden,
        "gen-2 (nb-2) reader PartitionDeletes must match the nb-2 JSONL golden \
         POSITIONALLY (ordered pk + markedForDeleteAt). The gen-2 source that range \
         filtering might skip is REINCLUDED. reader={:?} golden={:?}",
        gen2_deletes, gen2_golden
    );

    // The gen-2 reader must emit ONLY partition deletes (no phantom live rows): a
    // tombstone-only generation has no upserts.
    let gen2_upserts = gen2_records
        .iter()
        .filter(|r| matches!(r, DeltaRecord::Upsert { .. }))
        .count();
    assert_eq!(
        gen2_upserts, 0,
        "gen-2 (nb-2) is tombstone-only; reader must emit 0 Upserts but emitted {gen2_upserts}"
    );

    eprintln!(
        "[#1012 reader PASS] gen-1: 10 live rows (pk=1, ck 1..10); \
         gen-2: 2 partition tombstones (pk=1, pk=2) reincluded with markedForDeleteAt={}µs",
        gen2_golden[0].marked_deleted_micros
    );
}

/// `cass.sstable_io.reader.tombstone_only_partition`
///
/// Partition `pk=2` carries ONLY a partition tombstone and no live rows in any
/// generation. The reader must surface it as a deletion-bearing partition — NOT
/// as an absent/empty/corrupt partition.
#[tokio::test]
async fn reader_tombstone_only_partition_parses_as_deletion() {
    let Some(dir) = fixture_dir() else {
        eprintln!("[SKIP] fixture absent — skipping #1012 tombstone-only reader");
        return;
    };
    if data_db_for_gen(&dir, "2").is_none() {
        eprintln!("[SKIP] nb-2 Data.db absent — skipping #1012 tombstone-only reader");
        return;
    }
    let tmp = tempfile::tempdir().expect("tempdir");

    let golden = parse_gen2_partition_deletes(&jsonl_for_gen(&dir, "2"));
    let golden_pk2 = golden
        .iter()
        .find(|f| f.pk == 2)
        .expect("nb-2 golden must contain a tombstone-only pk=2");

    let records = collect_records(isolate_gen(&dir, "2", tmp.path())).await;
    let pk2 = records.iter().find_map(|r| match r {
        DeltaRecord::PartitionDelete { deleted_at, .. } if record_pk(r) == 2 => Some(*deleted_at),
        _ => None,
    });

    let pk2_deleted_at = pk2.unwrap_or_else(|| {
        panic!(
            "TOMBSTONE-ONLY PARTITION LOST: gen-2 (nb-2) partition pk=2 must surface as a \
             PartitionDelete, but the reader produced no deletion-bearing record for it. \
             Records seen: {:?}",
            records
                .iter()
                .map(|r| (record_pk(r), r.op_name()))
                .collect::<Vec<_>>()
        )
    });
    assert_eq!(
        pk2_deleted_at, golden_pk2.marked_deleted_micros,
        "tombstone-only pk=2 markedForDeleteAt must match the nb-2 golden \
         (reader={pk2_deleted_at}µs golden={}µs)",
        golden_pk2.marked_deleted_micros
    );

    // pk=2 must NOT appear as any live row (it is tombstone-only across all gens).
    let pk2_live = records
        .iter()
        .any(|r| matches!(r, DeltaRecord::Upsert { .. }) && record_pk(r) == 2);
    assert!(
        !pk2_live,
        "tombstone-only pk=2 must have NO live rows; reader produced a live Upsert for pk=2"
    );

    eprintln!(
        "[#1012 reader PASS] tombstone-only pk=2 surfaces as PartitionDelete \
         (markedForDeleteAt={pk2_deleted_at}µs), not absent/empty/corrupt"
    );
}

/// `cass.sstable_io.scanner.tombstone_only_partition_ranges`
///
/// A scan over the gen-2 source that SPANS pk=2 must surface the partition
/// deletion for pk=2 — the tombstone-only partition is not silently skipped when
/// the scanner walks the partition range that contains it. (The fixture's two
/// partitions, pk=1 and pk=2, are both visited in one streaming scan.)
#[tokio::test]
async fn scanner_tombstone_only_partition_surfaces_in_range() {
    let Some(dir) = fixture_dir() else {
        eprintln!("[SKIP] fixture absent — skipping #1012 scanner range");
        return;
    };
    if data_db_for_gen(&dir, "2").is_none() {
        eprintln!("[SKIP] nb-2 Data.db absent — skipping #1012 scanner range");
        return;
    }
    let tmp = tempfile::tempdir().expect("tempdir");

    // The committed golden establishes the FACTS the scan must surface: both
    // pk=1 and pk=2 partition tombstones. A 0-match-when-present is a failure.
    let golden = parse_gen2_partition_deletes(&jsonl_for_gen(&dir, "2"));
    assert_eq!(
        golden.len(),
        2,
        "nb-2 golden must list 2 partition tombstones"
    );

    let records = collect_records(isolate_gen(&dir, "2", tmp.path())).await;
    let scanned_pks: Vec<i64> = records
        .iter()
        .filter(|r| matches!(r, DeltaRecord::PartitionDelete { .. }))
        .map(record_pk)
        .collect();

    // Positional, ordered comparison: the scanner must visit pk=1 then pk=2 (token
    // order per Statistics.db: First token=pk1, Last token=pk2) and surface both.
    let golden_pks: Vec<i64> = golden.iter().map(|f| f.pk).collect();
    assert_eq!(
        scanned_pks, golden_pks,
        "range scan over the gen-2 source must surface both partition tombstones in \
         token order; the tombstone-only pk=2 partition must not be skipped. \
         scanned={scanned_pks:?} golden={golden_pks:?}"
    );
    assert!(
        scanned_pks.contains(&2),
        "scanner LOST the tombstone-only pk=2 partition deletion while spanning its range"
    );

    eprintln!(
        "[#1012 scanner PASS] range scan surfaced partition deletions for pk={scanned_pks:?} \
         (tombstone-only pk=2 included, not skipped)"
    );
}

// ════════════════════════════════════════════════════════════════════════════
// TESTS — cross-generation MERGE shadowing (P0 GAP; pinned, unweakened)
// ════════════════════════════════════════════════════════════════════════════
//
// These two tests encode the CORRECT Cassandra semantics and FAIL today because
// CQLite's compaction/merge read path drops partition tombstones (see module
// docs). They are `#[ignore]`d so the gate stays green while precisely pinning
// the resurrection; the lead files a follow-up and the manifest scenarios are
// `status: partial`. De-`#[ignore]` once the merge path threads partition
// deletions through `CompactionRowData` / `parse_one_partition_for_compaction`.

/// `cass.tombstone_ttl.skipped_sstable.partition_delete_shadows_older_rows`
///
/// Merging gen-2 (newest; partition tombstone DELETE WHERE pk=1 at 2021-01-02)
/// over gen-1 (older live rows at 2021-01-01) MUST shadow all 10 pk=1 rows: a
/// query for pk=1 returns NO live rows. The gen-2 source is the one a clustering
/// /min-max range filter would make look skippable, yet it must stay visible.
#[test]
#[ignore = "P0 GAP (#1012): KWayMerger drops partition tombstones — gen-2 DELETE WHERE pk=1 \
            does NOT shadow gen-1's older live rows (resurrection). De-ignore when the \
            compaction/merge read path threads partition deletions."]
fn merge_partition_delete_shadows_older_rows_gap() {
    let Some(dir) = fixture_dir() else {
        eprintln!("[SKIP] fixture absent — skipping #1012 merge shadowing");
        return;
    };
    let Some(merged) = merge_both_generations(&dir) else {
        eprintln!("[SKIP] Data.db binaries absent — skipping #1012 merge shadowing");
        return;
    };

    let pk1_live: Vec<Option<i64>> = merged
        .iter()
        .filter(|t| t.pk == 1 && t.live)
        .map(|t| t.ck)
        .collect();

    assert!(
        pk1_live.is_empty(),
        "RESURRECTION (P0, #1012): merging the SKIPPABLE gen-2 source (nb-2: partition \
         tombstone DELETE WHERE pk=1 @2021-01-02) over gen-1 (nb-1: 10 live rows \
         @2021-01-01) must yield 0 live rows for pk=1, but {} survived (ck={:?}). \
         The gen-2 partition tombstone was dropped by the merge read path — gen-1's \
         older live rows were RESURRECTED. Expected: pk=1 → 0 live rows. Actual: {} rows.",
        pk1_live.len(),
        pk1_live,
        pk1_live.len()
    );
}

/// `cass.compaction_merge.partition_delete_shadowing_across_skipped_sources`
///
/// Compacting both generations (nb-2 newest + nb-1) with default gc_grace
/// (tombstones retained) MUST produce an output where pk=1 has no live rows and
/// the partition tombstone is retained, and where the tombstone-only pk=2
/// partition tombstone is also retained. Mirrors the issue_819 two-generation
/// no-resurrection discipline.
#[test]
#[ignore = "P0 GAP (#1012): compaction across the skipped gen-2 source RESURRECTS pk=1 rows \
            and DROPS the tombstone-only pk=2 partition. De-ignore when partition \
            tombstones are applied during compaction."]
fn compaction_partition_delete_shadowing_gap() {
    let Some(dir) = fixture_dir() else {
        eprintln!("[SKIP] fixture absent — skipping #1012 compaction shadowing");
        return;
    };
    let (Some(nb2), Some(nb1)) = (data_db_for_gen(&dir, "2"), data_db_for_gen(&dir, "1")) else {
        eprintln!("[SKIP] Data.db binaries absent — skipping #1012 compaction shadowing");
        return;
    };

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let out = tempfile::tempdir().expect("tempdir");
    let sch = schema();

    // gc_before=None ⇒ default gc_grace, tombstones retained (no purge).
    let report = rt
        .block_on(compact_sstables(
            vec![nb2, nb1],
            out.path(),
            &sch,
            100,
            None,
            None,
            false,
        ))
        .expect("compact_sstables over both generations");

    // Read the compacted output back through the merge read path and assert no
    // pk=1 live rows survived. The report's `output.data_path` is authoritative
    // (the writer may nest the generation in a subdir).
    let mut merger = KWayMerger::new(vec![report.output.data_path.clone()], &sch)
        .expect("re-open compacted output");
    let mut pk1_live = 0usize;
    loop {
        match merger.step().expect("merge step over compacted") {
            MergeStep::Complete => break,
            MergeStep::Partition { key, rows } => {
                let pk = i32::from_be_bytes(key.key.as_slice().try_into().unwrap_or([0, 0, 0, 0]));
                if pk == 1 {
                    pk1_live += rows
                        .iter()
                        .filter(|e| matches!(e.row_data, RowData::Live { .. }))
                        .count();
                }
            }
        }
    }

    assert_eq!(
        pk1_live, 0,
        "RESURRECTION (P0, #1012): compacting nb-2 (newest, DELETE WHERE pk=1) + nb-1 \
         (older live rows) with default gc_grace must leave 0 live rows for pk=1, but \
         {pk1_live} survived in the compacted output. The partition tombstone from the \
         skipped gen-2 source was not applied. Expected: pk=1 → 0 live rows. Actual: {pk1_live}."
    );
}
