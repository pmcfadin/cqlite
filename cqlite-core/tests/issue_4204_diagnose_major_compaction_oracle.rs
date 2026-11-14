//! Issue #4204 (owner ruling 2026-11-14): `reclaim_at_now` is the TABLE-WIDE dry major-compaction
//! prediction. Oracle: on a temp copy of a real Cassandra-written, multi-generation `test_tomb`
//! table (a tombstone in the newer generation that shadows rows of the older), run the REAL major
//! compaction (`compact_sstables`, `purge_safe = true`, the path `compact --major` calls) with the
//! same gc/now settings and assert diagnose's `rows_in` / `rows_out` / `dropped_whole` against what
//! that compaction actually read, wrote and dropped. Per-row counts, so an empty table cannot pass.

#![cfg(all(feature = "write-support", not(feature = "tombstones")))]

#[path = "support/datasets_root.rs"]
mod datasets_root;
#[path = "support/rebuild_fixtures.rs"]
mod rebuild_fixtures;

use std::path::{Path, PathBuf};

use cqlite_core::storage::sstable::diagnose::{diagnose_table, DiagnoseOptions, SourcedField};
use cqlite_core::storage::write_engine::merge::compact_sstables;

const NOW: i64 = 1_794_545_000;

fn strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

fn fixture(table: &str) -> Option<PathBuf> {
    match datasets_root::resolve_table_generation_dir("test_tomb", table) {
        Ok(d) => Some(d),
        Err(e) if strict() => {
            panic!("test_tomb.{table}: missing under CQLITE_REQUIRE_FIXTURES: {e}")
        }
        Err(e) => {
            eprintln!("SKIP test_tomb.{table}: {e}");
            None
        }
    }
}

fn copy_table(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).expect("mkdir");
    for e in std::fs::read_dir(src).expect("read_dir").flatten() {
        if e.path().is_file() {
            std::fs::copy(e.path(), dst.join(e.file_name())).expect("copy");
        }
    }
}

/// Rows the golden shows for one generation, as `--deep` streams them: every `rows[]` element
/// (clustering rows, range markers) plus one per partition-level deletion.
fn golden_rows(dir: &Path, generation: &str) -> u64 {
    let path = dir.join(format!("nb-{generation}-big-Data.db.jsonl"));
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("golden {}: {e}", path.display()))
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| {
            let v: serde_json::Value = serde_json::from_str(l).expect("golden line");
            let rows = v["rows"].as_array().map_or(0, |r| r.len() as u64);
            let pdel = u64::from(v["partition"].get("deletion_info").is_some());
            rows + pdel
        })
        .sum()
}

async fn check(table: &str, expect_in: u64, expect_out: u64) {
    let Some(src) = fixture(table) else { return };
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().join("tbl");
    copy_table(&src, &dir);
    let schema = rebuild_fixtures::table_schema("tombstone-parity.cql", table, "test_tomb");

    let report = diagnose_table(
        &dir,
        &DiagnoseOptions {
            now_secs: NOW,
            deep: true,
            top_n: 5,
            gc_grace_seconds: Some(0),
            schema: Some(schema.clone()),
        },
    )
    .await
    .expect("diagnose");
    let SourcedField::Measured { value: Some(p), .. } = report
        .reclaim_at_now
        .clone()
        .expect("deep run carries the table-wide prediction")
    else {
        panic!(
            "{table}: table-wide reclaim unmeasured: {:?}",
            report.reclaim_at_now
        )
    };

    // The REAL major compaction over the same inputs (newest first), same gc/now, purge-safe.
    let mut inputs: Vec<(u64, PathBuf)> = std::fs::read_dir(&dir)
        .expect("dir")
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.to_string_lossy().ends_with("-Data.db"))
        .map(|p| {
            let n = p.file_name().and_then(|n| n.to_str()).expect("name");
            (
                n.split('-')
                    .nth(1)
                    .expect("gen")
                    .parse::<u64>()
                    .expect("int gen"),
                p,
            )
        })
        .collect();
    assert!(
        inputs.len() >= 2,
        "{table}: need a multi-generation table, got {}",
        inputs.len()
    );
    inputs.sort_by(|a, b| b.0.cmp(&a.0));
    let out = tmp.path().join("out");
    let real = compact_sstables(
        inputs.iter().map(|(_, p)| p.clone()).collect(),
        &out,
        &schema,
        9,
        Some(NOW),
        Some(NOW),
        true,
    )
    .await
    .expect("real major compaction");

    // rows OUT: what the real compaction wrote, confirmed by an independent read of its output.
    assert_eq!(
        p.rows_out, real.stats.output_rows,
        "{table}: rows_out vs the real compaction"
    );
    let out_table = out.join("test_tomb");
    let out_dir = std::fs::read_dir(&out_table)
        .expect("output keyspace dir")
        .flatten()
        .map(|e| e.path())
        .next()
        .expect("output table dir");
    let written = diagnose_table(
        &out_dir,
        &DiagnoseOptions {
            now_secs: NOW,
            deep: true,
            top_n: 1,
            gc_grace_seconds: Some(0),
            schema: Some(schema.clone()),
        },
    )
    .await
    .expect("diagnose the real output");
    let written_rows: u64 = written
        .generations
        .iter()
        .map(|g| g.deep.as_ref().expect("deep").rows_scanned)
        .sum();
    assert_eq!(
        p.rows_out, written_rows,
        "{table}: rows_out vs a scan of the real output"
    );

    // rows IN: the generations the real compaction merged (not the dropped-whole ones), from the goldens.
    let dropped: Vec<String> = real
        .stats
        .dropped_whole
        .iter()
        .map(|d| {
            d.file_name()
                .and_then(|n| n.to_str())
                .expect("name")
                .split('-')
                .nth(1)
                .expect("gen")
                .to_string()
        })
        .collect();
    let mut want_dropped = dropped.clone();
    want_dropped.sort();
    let mut got_dropped = p.dropped_whole.clone();
    got_dropped.sort();
    assert_eq!(
        got_dropped, want_dropped,
        "{table}: dropped_whole vs the real compaction"
    );
    let want_in: u64 = inputs
        .iter()
        .map(|(g, _)| g.to_string())
        .filter(|g| !dropped.contains(g) || p.merged_generations.contains(g))
        .map(|g| golden_rows(&src, &g))
        .sum();
    assert_eq!(
        p.rows_in, want_in,
        "{table}: rows_in vs the goldens of the merged generations"
    );

    // Non-vacuity + the pinned numbers.
    assert!(p.rows_in > 0, "{table}: nothing was read: {p:?}");
    // A purge can legitimately leave nothing (both partitions deleted and past gc_grace); the real
    // compaction's own output confirmed that above, so only the table that keeps rows demands > 0.
    assert!(
        expect_out == 0 || p.rows_out > 0,
        "{table}: empty prediction {p:?}"
    );
    assert_eq!(
        (p.rows_in, p.rows_out),
        (expect_in, expect_out),
        "{table}: pinned counts"
    );
    eprintln!(
        "{table}: rows_in {} rows_out {} merged {:?} dropped {:?}",
        p.rows_in, p.rows_out, p.merged_generations, p.dropped_whole
    );
}

#[tokio::test]
async fn table_wide_reclaim_equals_the_real_major_compaction_resurrection_gc0() {
    check("resurrection_gc0", 11, 4).await;
}

#[tokio::test]
async fn table_wide_reclaim_equals_the_real_major_compaction_skipped_partition_delete() {
    check("skipped_partition_delete", 12, 0).await;
}

/// No schema: the prediction is `unmeasured: <cause>`, never `0`.
#[tokio::test]
async fn without_a_schema_the_table_wide_reclaim_is_unmeasured_with_a_cause() {
    let Some(src) = fixture("resurrection_gc0") else {
        return;
    };
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().join("tbl");
    copy_table(&src, &dir);
    let report = diagnose_table(
        &dir,
        &DiagnoseOptions {
            now_secs: NOW,
            deep: true,
            top_n: 1,
            gc_grace_seconds: Some(0),
            schema: None,
        },
    )
    .await
    .expect("diagnose");
    match report.reclaim_at_now.expect("deep") {
        SourcedField::Unmeasured { cause } => assert!(cause.contains("--schema"), "{cause}"),
        other => panic!("expected unmeasured, got {other:?}"),
    }
}
