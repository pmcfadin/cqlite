//! Issue #4204 — `diagnose` cheap tier (spec R2.1, R2.2, R5.1).
//!
//! ORACLE (R2.1): `sstablemetadata` run by the pinned `cassandra-5.0.8` tool against the SAME
//! committed generation, captured under `test-data/oracles/issue_4204/` (tool image and capture
//! instant recorded in each file; see `test-data/scripts/capture-issue-4204-sstablemetadata.sh`).
//! The tool prints `Estimated droppable tombstones` at `gcBefore = <capture instant>`, so each case
//! runs `diagnose` with `now = captured_epoch` and `gc_grace_seconds = 0`.
//!
//! R5.1 is proven on the FILES, not on a counter: the table dir is copied with every `Data.db`
//! replaced by an EMPTY file, and the cheap tier must produce the identical report — it cannot
//! have read a byte of Data.db.

#![cfg(all(feature = "write-support", not(feature = "tombstones")))]

#[path = "support/datasets_root.rs"]
mod datasets_root;

use std::path::{Path, PathBuf};

use cqlite_core::storage::sstable::diagnose::{
    diagnose_table, DiagnoseOptions, GenerationDiagnosis, SourcedField,
};

fn strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

fn oracle_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../test-data/oracles/issue_4204")
}

struct Oracle {
    keyspace: String,
    table: String,
    generation: u64,
    format: String,
    epoch: i64,
    min_ts: i64,
    max_ts: i64,
    min_ldt: i64,
    max_ldt: i64,
    /// `None` when the tool prints no `Compression ratio` line (an uncompressed SSTable).
    compression_ratio: Option<f64>,
    repaired_at: i64,
    pending_repair: Option<String>,
    droppable: f64,
}

fn paren_number(line: &str) -> i64 {
    let inner = line
        .rsplit('(')
        .next()
        .and_then(|s| s.strip_suffix(')'))
        .unwrap_or_else(|| panic!("no (number) in {line:?}"));
    inner.parse().unwrap_or_else(|e| panic!("{line:?}: {e}"))
}

fn parse_oracle(path: &Path) -> Oracle {
    let name = path
        .file_name()
        .expect("name")
        .to_string_lossy()
        .into_owned();
    // <keyspace>.<table>.<fmt>-<gen>-<kind>.sstablemetadata.txt
    let stem = name.trim_end_matches(".sstablemetadata.txt");
    let mut parts = stem.splitn(3, '.');
    let (keyspace, table, sst) = (
        parts.next().expect("ks").to_string(),
        parts.next().expect("table").to_string(),
        parts.next().expect("sstable").to_string(),
    );
    let mut sst_parts = sst.split('-');
    let format = sst_parts.next().expect("format").to_string();
    let generation: u64 = sst_parts.next().expect("gen").parse().expect("numeric gen");
    let text = std::fs::read_to_string(path).expect("oracle");
    let get = |prefix: &str| -> String {
        text.lines()
            .find(|l| l.starts_with(prefix))
            .unwrap_or_else(|| panic!("{path:?}: no `{prefix}` line"))
            .to_string()
    };
    let epoch: i64 = get("# captured_epoch:")
        .trim_start_matches("# captured_epoch:")
        .split_whitespace()
        .next()
        .expect("epoch")
        .parse()
        .expect("epoch number");
    let value = |prefix: &str| get(prefix)[prefix.len()..].trim().to_string();
    let pending = value("Pending repair:");
    Oracle {
        keyspace,
        table,
        generation,
        format,
        epoch,
        min_ts: paren_number(&get("Minimum timestamp:")),
        max_ts: paren_number(&get("Maximum timestamp:")),
        min_ldt: paren_number(&get("SSTable min local deletion time:")),
        max_ldt: paren_number(&get("SSTable max local deletion time:")),
        compression_ratio: text
            .lines()
            .find(|l| l.starts_with("Compression ratio:"))
            .map(|l| {
                l["Compression ratio:".len()..]
                    .trim()
                    .parse()
                    .expect("ratio")
            }),
        repaired_at: value("Repaired at:").parse().expect("repaired"),
        pending_repair: (pending != "--").then(|| pending.replace('-', "")),
        droppable: value("Estimated droppable tombstones:")
            .parse()
            .expect("droppable"),
    }
}

fn oracles() -> Vec<Oracle> {
    let mut v: Vec<Oracle> = std::fs::read_dir(oracle_dir())
        .expect("committed oracle dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.to_string_lossy().ends_with(".sstablemetadata.txt"))
        .map(|p| parse_oracle(&p))
        .collect();
    v.sort_by(|a, b| {
        (&a.keyspace, &a.table, a.generation).cmp(&(&b.keyspace, &b.table, b.generation))
    });
    assert!(v.len() >= 6, "oracle set must be present, got {}", v.len());
    v
}

fn table_dir(keyspace: &str, table: &str) -> Option<PathBuf> {
    match datasets_root::resolve_table_generation_dir(keyspace, table) {
        Ok(d) => Some(d),
        Err(e) if strict() => {
            panic!("{keyspace}.{table}: fixture missing under CQLITE_REQUIRE_FIXTURES: {e}")
        }
        Err(e) => {
            eprintln!("SKIP {keyspace}.{table}: {e}");
            None
        }
    }
}

fn opts(now: i64) -> DiagnoseOptions {
    DiagnoseOptions {
        now_secs: now,
        deep: false,
        top_n: 5,
        gc_grace_seconds: Some(0),
        schema: None,
    }
}

fn measured<T: Clone>(f: &SourcedField<T>, what: &str) -> Option<T> {
    match f {
        SourcedField::Measured { value, .. } => value.clone(),
        SourcedField::Unmeasured { cause } => panic!("{what} unmeasured: {cause}"),
    }
}

fn gen_of<'a>(gens: &'a [GenerationDiagnosis], o: &Oracle) -> &'a GenerationDiagnosis {
    gens.iter()
        .find(|g| g.generation == o.generation && g.format == o.format)
        .unwrap_or_else(|| {
            panic!(
                "{}.{}: no {}-{} generation in the report",
                o.keyspace, o.table, o.format, o.generation
            )
        })
}

/// R2.1: every cheap-tier field equals the captured `sstablemetadata` value, per generation.
#[tokio::test]
async fn cheap_tier_fields_match_the_sstablemetadata_oracle() {
    let mut checked = 0;
    for o in oracles() {
        let Some(dir) = table_dir(&o.keyspace, &o.table) else {
            continue;
        };
        let report = diagnose_table(&dir, &opts(o.epoch))
            .await
            .expect("diagnose");
        let g = gen_of(&report.generations, &o);
        let case = format!("{}.{} {}-{}", o.keyspace, o.table, o.format, o.generation);
        assert_eq!(g.min_timestamp, o.min_ts, "{case}: min timestamp");
        assert_eq!(
            measured(&g.max_timestamp, "max_timestamp"),
            Some(o.max_ts),
            "{case}: max timestamp"
        );
        assert_eq!(
            g.min_local_deletion_time, o.min_ldt,
            "{case}: min local deletion time"
        );
        assert_eq!(
            g.max_local_deletion_time, o.max_ldt,
            "{case}: max local deletion time"
        );
        match o.compression_ratio {
            Some(want) => {
                let ratio = measured(&g.compression_ratio, "compression_ratio").expect("value");
                assert!(
                    (ratio - want).abs() < 1e-12,
                    "{case}: compression ratio {ratio} vs {want}"
                );
            }
            // The tool prints no ratio line for an uncompressed SSTable (NO_COMPRESSION_RATIO =
            // -1): diagnose must say so, never surface the -1 sentinel as a number.
            None => assert!(
                matches!(g.compression_ratio, SourcedField::Unmeasured { .. }),
                "{case}: uncompressed generation must report an unmeasured ratio, got {:?}",
                g.compression_ratio
            ),
        }
        assert_eq!(g.repaired_at, o.repaired_at, "{case}: repaired at");
        assert_eq!(
            measured(&g.pending_repair, "pending_repair"),
            o.pending_repair,
            "{case}: pending repair"
        );
        let d = measured(&g.estimated_droppable_tombstone_ratio, "droppable ratio").expect("value");
        assert!(
            (d - o.droppable).abs() < 1e-12,
            "{case}: estimated droppable tombstones {d} vs {}",
            o.droppable
        );
        checked += 1;
    }
    if strict() {
        assert!(
            checked >= 6,
            "every oracle case must run under CQLITE_REQUIRE_FIXTURES, ran {checked}"
        );
    }
}

/// R2.2: `fully_expired_at_now` is `max_local_deletion_time < gcBefore`, re-derived here from the
/// oracle's own printed max LDT (strict `<`, #1385).
#[tokio::test]
async fn fully_expired_at_now_follows_the_strict_gc_before_boundary() {
    let o = oracles()
        .into_iter()
        .find(|o| o.table == "skipped_partition_delete" && o.generation == 2)
        .expect("oracle for skipped_partition_delete gen 2");
    assert!(
        o.max_ldt < i64::MAX,
        "precondition: the generation carries tombstones"
    );
    let Some(dir) = table_dir(&o.keyspace, &o.table) else {
        return;
    };
    for (now, want) in [
        (o.max_ldt, false),
        (o.max_ldt + 1, true),
        (o.max_ldt - 5, false),
        (o.max_ldt + 86_400, true),
    ] {
        let report = diagnose_table(&dir, &opts(now)).await.expect("diagnose");
        let g = gen_of(&report.generations, &o);
        assert_eq!(
            g.fully_expired_at_now, want,
            "now={now} gcBefore={now}: max_ldt={}",
            o.max_ldt
        );
        assert_eq!(
            g.fully_expired_at_now,
            o.max_ldt < now,
            "independent re-derivation"
        );
    }
    // A generation with NO tombstones (max LDT = sentinel) is never fully expired.
    let o2 = oracles()
        .into_iter()
        .find(|o| o.table == "simple_table" && o.keyspace == "test_basic")
        .expect("simple_table oracle");
    let Some(dir2) = table_dir(&o2.keyspace, &o2.table) else {
        return;
    };
    let r2 = diagnose_table(&dir2, &opts(i64::MAX / 2))
        .await
        .expect("diagnose");
    assert!(!gen_of(&r2.generations, &o2).fully_expired_at_now);
}

fn copy_dir_with_empty_data_db(src: &Path) -> tempfile::TempDir {
    let tmp = tempfile::tempdir().expect("tempdir");
    for e in std::fs::read_dir(src).expect("read fixture dir") {
        let p = e.expect("entry").path();
        let name = p.file_name().expect("name").to_string_lossy().into_owned();
        if !p.is_file() || name.ends_with(".jsonl") || name.ends_with(".txt.jsonl") {
            continue;
        }
        let dest = tmp.path().join(&name);
        if name.ends_with("-Data.db") {
            std::fs::write(&dest, b"").expect("empty Data.db");
        } else {
            std::fs::copy(&p, &dest).expect("copy component");
        }
    }
    tmp
}

/// R5.1: the cheap tier NEVER opens Data.db — with every Data.db replaced by an EMPTY file the
/// report is field-for-field identical — and carries no consistency verdict (the report type has
/// no such field; only values and their sources).
#[tokio::test]
async fn cheap_tier_never_reads_data_db() {
    for (ks, table) in [
        ("test_basic", "simple_table"),
        ("test_tomb", "skipped_partition_delete"),
        ("test_da", "simple_table"),
    ] {
        let Some(dir) = table_dir(ks, table) else {
            continue;
        };
        let real = diagnose_table(&dir, &opts(1_794_545_000))
            .await
            .expect("real");
        let hollow_dir = copy_dir_with_empty_data_db(&dir);
        let hollow = diagnose_table(hollow_dir.path(), &opts(1_794_545_000))
            .await
            .unwrap_or_else(|e| panic!("{ks}.{table}: cheap tier needed Data.db content: {e}"));
        assert_eq!(
            real.generations.len(),
            hollow.generations.len(),
            "{ks}.{table}"
        );
        for (a, b) in real.generations.iter().zip(&hollow.generations) {
            assert_eq!(
                format!("{a:?}"),
                format!("{b:?}"),
                "{ks}.{table}: report differs when Data.db is empty"
            );
        }
        assert_eq!(
            real.token_overlap.counts, hollow.token_overlap.counts,
            "{ks}.{table}: token overlap"
        );
    }
}
