//! Issue #4195 — exact `Pass` per case for the Index/Summary/Bloom audit rows.
//!
//! `healthy_corpus_has_no_findings_and_no_failed_rows` (in
//! `issue_4195_component_audit.rs`) only proves these rows are not `Fail`, so a row
//! that silently became `Skip` on every healthy generation would stay green. Here each
//! healthy committed generation is asserted individually against the EXACT verdict its
//! own component layout implies. Expectations come from the generation's files on
//! disk (component presence, the `Summary.db` sample count read by `SummaryReader`),
//! never from the audit's own output. A generation that legitimately lacks a component
//! must show the documented `Skip` text, never a silent `Pass`.
//! There is no suite-wide `ran > 0`: every generation asserts on its own.

#[path = "support/datasets_root.rs"]
mod datasets_root;

use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::summary_reader::SummaryReader;
use cqlite_core::storage::sstable::verify::{verify_sstable_generation_audit, VerifyReport};
use cqlite_core::storage::sstable::verify_audit::AuditVerdict;
use cqlite_core::Config;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const A1: &str = "index-data-identity";
const A2: &str = "summary-index-correlation";
const A3: &str = "bloom-no-false-negative";

async fn platform() -> (Config, Arc<Platform>) {
    let config = Config::default();
    let p = Arc::new(Platform::new(&config).await.expect("platform"));
    (config, p)
}

async fn audit(data_db: &Path) -> VerifyReport {
    let (config, p) = platform().await;
    verify_sstable_generation_audit(data_db, false, &config, p)
        .await
        .unwrap_or_else(|e| panic!("audit {} returned Err: {e}", data_db.display()))
}

fn verdict(r: &VerifyReport, id: &str) -> AuditVerdict {
    r.audit
        .as_ref()
        .expect("audit mode yields an AuditSummary")
        .verdict(id)
        .unwrap_or_else(|| panic!("no row {id}"))
        .clone()
}

/// Every `*-Data.db` under `<root>/<keyspace>/<table-dir>/`, by an independent walk.
fn walk_generations(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let subdirs = |p: &Path| -> Vec<PathBuf> {
        std::fs::read_dir(p)
            .expect("dir readable")
            .map(|e| e.expect("entry").path())
            .filter(|p| p.is_dir())
            .collect()
    };
    for ks in subdirs(root) {
        for t in subdirs(&ks) {
            for f in std::fs::read_dir(&t).expect("table dir").flatten() {
                let f = f.path();
                if f.to_string_lossy().ends_with("-Data.db") {
                    out.push(f);
                }
            }
        }
    }
    out.sort();
    out
}

fn corpus_root() -> PathBuf {
    datasets_root::sstables_root_candidates()
        .into_iter()
        .find(|r| r.join("test_basic").is_dir() && !walk_generations(r).is_empty())
        .expect("no sstables root with data: fetch the datasets (CQLITE_REQUIRE_FIXTURES=1)")
}

fn sibling(data_db: &Path, component: &str) -> PathBuf {
    let s = data_db.to_string_lossy();
    PathBuf::from(
        s.strip_suffix("Data.db")
            .unwrap_or_else(|| panic!("not a Data.db: {s}")),
    )
    .with_file_name(format!(
        "{}{component}",
        data_db
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(|n| n.strip_suffix("Data.db"))
            .expect("Data.db file name")
    ))
}

#[derive(PartialEq, Clone, Copy, Debug)]
enum Fmt {
    Big,
    Bti,
}

fn fmt_of(data_db: &Path) -> Fmt {
    let n = data_db.to_string_lossy();
    if n.ends_with("-big-Data.db") {
        Fmt::Big
    } else if n.ends_with("-bti-Data.db") {
        Fmt::Bti
    } else {
        panic!("unrecognised format in {n}");
    }
}

async fn sample_count(summary: &Path) -> usize {
    let (_, p) = platform().await;
    SummaryReader::open(summary, p)
        .await
        .unwrap_or_else(|e| panic!("healthy Summary.db {} unreadable: {e}", summary.display()))
        .get_entries()
        .len()
}

/// The exact verdict the generation's own files imply for `row`, or `None` when the
/// row is out of scope for the generation's format.
async fn expected(data_db: &Path, row: &str) -> Option<AuditVerdict> {
    let f = fmt_of(data_db);
    let skip = |s: &str| Some(AuditVerdict::Skip(s.to_string()));
    match (row, f) {
        (A1, Fmt::Big) | (A2, Fmt::Big) if !sibling(data_db, "Index.db").exists() => {
            skip("boundary source unreadable")
        }
        (A1, Fmt::Big) => Some(AuditVerdict::Pass),
        (A2, Fmt::Big) => {
            let s = sibling(data_db, "Summary.db");
            if !s.exists() {
                skip("no Summary.db")
            } else if sample_count(&s).await == 0 {
                skip("Summary.db carries no samples")
            } else {
                Some(AuditVerdict::Pass)
            }
        }
        (A3, _) => {
            if sibling(data_db, "Filter.db").exists() {
                Some(AuditVerdict::Pass)
            } else if f == Fmt::Big && !sibling(data_db, "Index.db").exists() {
                skip("boundary source unreadable")
            } else {
                skip("no Filter.db")
            }
        }
        _ => None,
    }
}

/// Audit every generation of the corpus, asserting `row` per case. Returns the
/// number of generations that carry the row's component (for the caller to name).
async fn assert_row_per_generation(row: &str, want_fmt: &[Fmt]) -> Vec<String> {
    let root = corpus_root();
    let gens = walk_generations(&root);
    assert!(
        gens.len() >= 100,
        "expected the full datasets-v4 corpus, found {}",
        gens.len()
    );
    let mut violations = Vec::new();
    for data in gens.iter().filter(|g| want_fmt.contains(&fmt_of(g))) {
        let want = expected(data, row).await.expect("row in scope");
        let got = verdict(&audit(data).await, row);
        if got != want {
            violations.push(format!(
                "{} {row}: expected {want:?}, got {got:?}",
                data.display()
            ));
        }
    }
    violations
}

/// A1.1 on every BIG generation: `Pass` (Index.db present), never `Skip`.
#[tokio::test]
async fn a1_1_index_data_identity_passes_on_every_healthy_big_generation() {
    let v = assert_row_per_generation(A1, &[Fmt::Big]).await;
    assert!(v.is_empty(), "{} case(s) wrong:\n{}", v.len(), v.join("\n"));
}

/// A2.1 on every BIG generation: `Pass` where Summary.db has samples, else the
/// documented Skip text.
#[tokio::test]
async fn a2_1_summary_index_correlation_passes_on_every_healthy_big_generation() {
    let v = assert_row_per_generation(A2, &[Fmt::Big]).await;
    assert!(v.is_empty(), "{} case(s) wrong:\n{}", v.len(), v.join("\n"));
}

/// A3.1 on every BIG and BTI generation: `Pass` where Filter.db exists, else
/// `Skip("no Filter.db")`.
#[tokio::test]
async fn a3_1_bloom_no_false_negative_passes_on_every_healthy_generation() {
    let v = assert_row_per_generation(A3, &[Fmt::Big, Fmt::Bti]).await;
    assert!(v.is_empty(), "{} case(s) wrong:\n{}", v.len(), v.join("\n"));
}

/// A3.1 on BTI specifically: the committed BTI generations exist and each reaches
/// `Pass` (guards a BTI-only regression hiding behind the BIG majority).
#[tokio::test]
async fn a3_1_bloom_passes_on_each_committed_bti_generation() {
    let root = corpus_root();
    let bti: Vec<PathBuf> = walk_generations(&root)
        .into_iter()
        .filter(|g| fmt_of(g) == Fmt::Bti)
        .collect();
    assert!(!bti.is_empty(), "no BTI generation in the corpus");
    for data in &bti {
        assert!(
            sibling(data, "Filter.db").exists(),
            "BTI fixture {} lacks Filter.db",
            data.display()
        );
        assert_eq!(
            verdict(&audit(data).await, A3),
            AuditVerdict::Pass,
            "{}",
            data.display()
        );
    }
}

/// A2.1 explicit per-case: a BIG fixture whose Summary.db has more than one sample,
/// so correlation is exercised on several samples, not one. (No `test_wide_rows`
/// generation qualifies: all have a single sample; `simple_table` has 8.)
async fn assert_multi_sample_summary(keyspace: &str, table: &str, min_samples: usize) {
    let root = datasets_root::sstables_root_for_table(keyspace, table)
        .unwrap_or_else(|| panic!("{}", datasets_root::describe_search(keyspace, table)));
    let dirs = datasets_root::table_generation_dirs(&root, keyspace, table);
    assert!(!dirs.is_empty(), "no generation for {keyspace}.{table}");
    for dir in dirs {
        let data = walk_generations_in(&dir);
        assert_eq!(fmt_of(&data), Fmt::Big, "{}", data.display());
        let summary = sibling(&data, "Summary.db");
        let n = sample_count(&summary).await;
        assert!(
            n >= min_samples && n > 1,
            "precondition: {} must have >1 (>= {min_samples}) Summary.db samples, has {n}",
            summary.display()
        );
        assert_eq!(
            verdict(&audit(&data).await, A2),
            AuditVerdict::Pass,
            "{}",
            data.display()
        );
    }
}

fn walk_generations_in(dir: &Path) -> PathBuf {
    std::fs::read_dir(dir)
        .expect("generation dir")
        .map(|e| e.expect("entry").path())
        .find(|p| p.to_string_lossy().ends_with("-Data.db"))
        .unwrap_or_else(|| panic!("no Data.db in {}", dir.display()))
}

#[tokio::test]
async fn a2_1_multi_sample_summary_simple_table_passes() {
    assert_multi_sample_summary("test_basic", "simple_table", 8).await;
}

#[tokio::test]
async fn a2_1_multi_sample_summary_log_entries_passes() {
    assert_multi_sample_summary("test_timeseries", "log_entries", 2).await;
}
