//! Issue #4195 — `verify --mode audit`: cross-component consistency.
//!
//! Expected outcomes are computed by the test from the CLEAN source files'
//! own structure (never from `verify_audit`'s output on the corrupt copy).
//! Fixtures are Cassandra 5.0.8 bytes from datasets-v4; corruption fixtures
//! are documented byte edits of them (see
//! `test-data/datasets/corruption/test_comp_corrupt/corruption-manifest.yml`).
//! Every case asserts independently; there is no suite-wide `ran > 0`.

#[path = "support/datasets_root.rs"]
mod datasets_root;

use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::verify::{verify_sstable_generation_audit, VerifyReport};
use cqlite_core::storage::sstable::verify_audit::AuditVerdict;
use cqlite_core::storage::sstable::version_gate::SsTableFormat;
use cqlite_core::Config;
use std::path::{Path, PathBuf};
use std::sync::Arc;

async fn audit_generation(data_db: &Path, deep: bool) -> VerifyReport {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    verify_sstable_generation_audit(data_db, deep, &config, platform)
        .await
        .unwrap_or_else(|e| panic!("audit {} returned Err: {e}", data_db.display()))
}

/// Every `<keyspace>/<table-dir>/*-Data.db` under a sstables root, found by an
/// independent directory walk.
fn all_generations(sstables_root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for ks in std::fs::read_dir(sstables_root).expect("sstables root readable") {
        let ks = ks.expect("ks entry").path();
        if !ks.is_dir() {
            continue;
        }
        for t in std::fs::read_dir(&ks).expect("keyspace readable") {
            let t = t.expect("table entry").path();
            if !t.is_dir() {
                continue;
            }
            for f in std::fs::read_dir(&t).expect("table dir readable") {
                let f = f.expect("file entry").path();
                if f.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.ends_with("-Data.db"))
                {
                    out.push(f);
                }
            }
        }
    }
    out.sort();
    out
}

fn fetched_sstables_root() -> PathBuf {
    datasets_root::sstables_root_candidates()
        .into_iter()
        .find(|r| r.join("test_basic").is_dir() && !all_generations(r).is_empty())
        .expect("no sstables root with data: fetch the datasets (CQLITE_REQUIRE_FIXTURES=1)")
}

fn describe(report: &VerifyReport) -> String {
    let rows = report
        .audit
        .as_ref()
        .map(|a| {
            a.rows
                .iter()
                .map(|r| format!("{}={:?}", r.invariant, r.verdict))
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_default();
    format!("findings={:?} rows=[{rows}]", report.findings)
}

/// Zero false positives: every generation of every healthy datasets-v4 table,
/// shallow and `--deep`, has no finding and no `Fail` row, exactly eight rows,
/// and the format-scoped applicability the design fixes.
#[tokio::test]
async fn healthy_corpus_has_no_findings_and_no_failed_rows() {
    let root = fetched_sstables_root();
    let gens = all_generations(&root);
    assert!(
        gens.len() >= 100,
        "expected the full datasets-v4 corpus, found {}",
        gens.len()
    );
    let mut violations = Vec::new();
    let mut audited = 0usize;
    let mut tally: std::collections::BTreeMap<(String, String), usize> = Default::default();
    for data in &gens {
        for deep in [false, true] {
            let r = audit_generation(data, deep).await;
            audited += 1;
            let a = r.audit.as_ref().expect("audit mode yields an AuditSummary");
            for row in &a.rows {
                let v = match &row.verdict {
                    AuditVerdict::Skip(c) => format!("Skip({c})"),
                    other => format!("{other:?}"),
                };
                *tally.entry((row.invariant.to_string(), v)).or_default() += 1;
            }
            let mut bad = Vec::new();
            if !r.findings.is_empty() {
                bad.push("has findings".to_string());
            }
            if a.rows.len() != 8 {
                bad.push("not 8 rows".into());
            }
            if a.any_fail() {
                bad.push("a row Failed".into());
            }
            if a.vacuous() {
                bad.push("vacuous (0 measured)".into());
            }
            match (
                r.format,
                a.verdict("bti-trie-bounds"),
                a.verdict("index-data-identity"),
            ) {
                (SsTableFormat::Big, Some(AuditVerdict::Skip(c)), Some(id))
                    if c == "not applicable: BIG format" && !matches!(id, AuditVerdict::Fail) => {}
                (SsTableFormat::Bti, Some(AuditVerdict::Pass), Some(AuditVerdict::Skip(c)))
                    if c == "not applicable: BTI format" => {}
                _ => bad.push("format applicability wrong".into()),
            }
            if a.verdict("statistics-vs-scan")
                == Some(&AuditVerdict::Skip("requires --deep".into()))
                && deep
            {
                bad.push("deep run still skipped statistics".into());
            }
            if !bad.is_empty() {
                violations.push(format!(
                    "{} (deep={deep}): {} :: {}",
                    data.display(),
                    bad.join("; "),
                    describe(&r)
                ));
            }
        }
    }
    for ((inv, v), n) in &tally {
        eprintln!("tally {inv:28} {v:70} {n}");
    }
    assert!(
        violations.is_empty(),
        "{} of {} audits wrong:\n{}",
        violations.len(),
        audited,
        violations.join("\n")
    );
}
