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
use cqlite_core::storage::sstable::verify::{
    verify_sstable, verify_sstable_audit, VerifyErrorClass, VerifyMode,
};
use cqlite_core::storage::sstable::verify::{verify_sstable_generation_audit, VerifyReport};
use cqlite_core::storage::sstable::verify_audit::{take_header_bytes_requested, AuditVerdict};
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

// ---------------------------------------------------------------------------
// Corruption fixtures
// ---------------------------------------------------------------------------

const NEW_FIXTURES: [&str; 6] = [
    "index_entry_offset_swap",
    "summary_sample_position_corrupt",
    "filter_db_bit_flip_bti",
    "bti_data_offset_out_of_bounds",
    "compression_info_short_coverage",
    "statistics_partition_count_mismatch",
];

fn manifest_text() -> String {
    let p = datasets_root::repo_root()
        .join("test-data/datasets/corruption/test_comp_corrupt/corruption-manifest.yml");
    std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("manifest {}: {e}", p.display()))
}

/// `field` of the manifest entry named `name`.
fn manifest_field(name: &str, field: &str) -> String {
    let text = manifest_text();
    let block = text
        .split("\n  - name: ")
        .find(|b| b.lines().next().is_some_and(|l| l.trim() == name))
        .unwrap_or_else(|| panic!("fixture {name} not in the committed manifest"));
    let prefix = format!("    {field}: ");
    block
        .lines()
        .find_map(|l| l.strip_prefix(prefix.as_str()))
        .unwrap_or_else(|| panic!("{name}: no {field}"))
        .trim()
        .trim_matches('"')
        .to_string()
}

fn sha256_hex(path: &Path) -> String {
    use std::process::Command;
    let out = Command::new("sha256sum")
        .arg(path)
        .output()
        .expect("sha256sum");
    String::from_utf8_lossy(&out.stdout)
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_string()
}

/// The six audit-only fixtures, generated ONCE by the committed generator from
/// the datasets-v4 clean sources, then bound to the committed manifest's
/// `corrupted_sha256` (a hash mismatch is a hard failure: the bytes under test
/// must be exactly the documented edit).
fn generated_root() -> &'static Path {
    static ROOT: std::sync::OnceLock<(tempfile::TempDir, PathBuf)> = std::sync::OnceLock::new();
    &ROOT
        .get_or_init(|| {
            let tmp = tempfile::tempdir().expect("tempdir");
            let out = tmp.path().join("test_comp_corrupt");
            let mut cmd = std::process::Command::new("python3");
            cmd.arg(
                datasets_root::repo_root()
                    .join("test-data/scripts/generate_audit_corruption_fixtures.py"),
            )
            .arg("--corrupt-dir")
            .arg(&out);
            for r in datasets_root::sstables_root_candidates() {
                cmd.arg("--root").arg(r);
            }
            let res = cmd
                .output()
                .expect("python3 must be available to build audit fixtures");
            assert!(
                res.status.success(),
                "audit fixture generation failed: {}",
                String::from_utf8_lossy(&res.stderr)
            );
            for name in NEW_FIXTURES {
                let file = manifest_field(name, "corrupted_path");
                let file = Path::new(&file).file_name().expect("file name").to_owned();
                let got = sha256_hex(&out.join(name).join(&file));
                assert_eq!(
                    got,
                    manifest_field(name, "corrupted_sha256"),
                    "{name}: generated bytes differ from the committed manifest"
                );
            }
            (tmp, out)
        })
        .1
}

/// Directory of a corruption fixture. The 12 pre-existing ones come from the
/// fetched/checked-out corpus (hard failure when absent), the 6 new from [`generated_root`].
fn fixture(name: &str) -> PathBuf {
    if NEW_FIXTURES.contains(&name) {
        return generated_root().join(name);
    }
    for r in datasets_root::sstables_root_candidates() {
        let d = r
            .parent()
            .expect("datasets root")
            .join("corruption/test_comp_corrupt")
            .join(name);
        if std::fs::read_dir(&d).is_ok_and(|mut it| {
            it.any(|e| e.is_ok_and(|e| e.file_name().to_string_lossy().ends_with("-Data.db")))
        }) {
            return d;
        }
    }
    panic!("corruption fixture {name} not found (fetch datasets-v4 / CQLITE_REQUIRE_FIXTURES=1)");
}

fn data_db(dir: &Path) -> PathBuf {
    std::fs::read_dir(dir)
        .expect("fixture dir")
        .map(|e| e.expect("entry").path())
        .find(|p| p.to_string_lossy().ends_with("-Data.db"))
        .unwrap_or_else(|| panic!("no Data.db in {}", dir.display()))
}

async fn audit_fixture(name: &str, deep: bool) -> VerifyReport {
    audit_generation(&data_db(&fixture(name)), deep).await
}

fn verdict(r: &VerifyReport, id: &str) -> AuditVerdict {
    r.audit
        .as_ref()
        .expect("audit")
        .verdict(id)
        .unwrap_or_else(|| panic!("no row {id}"))
        .clone()
}

fn only_class(
    r: &VerifyReport,
    class: VerifyErrorClass,
) -> Vec<&cqlite_core::storage::sstable::VerifyFinding> {
    r.findings.iter().filter(|f| f.class == class).collect()
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// Independent (test-side) Index.db walk: (entry offset, key, data offset).
fn clean_index_entries(index_db: &Path) -> Vec<(u64, Vec<u8>, u64)> {
    let b = std::fs::read(index_db).expect("Index.db");
    let vint = |at: usize| -> (u64, usize) {
        let first = b[at];
        let extra = first.leading_ones() as usize;
        if extra == 0 {
            return (u64::from(first), 1);
        }
        let mut v = u64::from(first & (0xffu8 >> (extra + 1)));
        for k in 0..extra {
            v = (v << 8) | u64::from(b[at + 1 + k]);
        }
        (v, 1 + extra)
    };
    let (mut i, mut out) = (0usize, Vec::new());
    while i < b.len() {
        let kl = usize::from(u16::from_be_bytes([b[i], b[i + 1]]));
        let key = b[i + 2..i + 2 + kl].to_vec();
        let (off, n) = vint(i + 2 + kl);
        let (promoted, m) = vint(i + 2 + kl + n);
        out.push((i as u64, key, off));
        i += 2 + kl + n + m + promoted as usize;
    }
    out
}

fn clean_dir(ks: &str, table: &str) -> PathBuf {
    datasets_root::resolve_table_generation_dir(ks, table)
        .unwrap_or_else(|e| panic!("clean source {ks}.{table}: {e}"))
}

fn component(dir: &Path, suffix: &str) -> PathBuf {
    std::fs::read_dir(dir)
        .expect("dir")
        .map(|e| e.expect("entry").path())
        .find(|p| p.to_string_lossy().ends_with(suffix))
        .unwrap_or_else(|| panic!("no *{suffix} in {}", dir.display()))
}

#[tokio::test]
async fn a1_2_swapped_index_offsets_are_caught_reading_only_headers() {
    let clean = clean_index_entries(&component(
        &clean_dir("test_basic", "simple_table"),
        "-Index.db",
    ));
    let dir = fixture("index_entry_offset_swap");
    take_header_bytes_requested();
    let r = audit_generation(&data_db(&dir), false).await;
    let header_bytes = take_header_bytes_requested();
    assert_eq!(
        verdict(&r, "index-data-identity"),
        AuditVerdict::Fail,
        "{}",
        describe(&r)
    );
    let found = only_class(&r, VerifyErrorClass::IndexPositionKeyMismatch);
    assert_eq!(
        found.len(),
        2,
        "exactly the two swapped entries: {}",
        describe(&r)
    );
    // Expected from the CLEAN source by the fixture's documented rule: the first
    // adjacent pair whose offset VInts have equal encoded length (7 data bits per
    // byte) and differ.
    let vlen = |v: u64| {
        (1..=9usize)
            .find(|n| v < (1u64 << (7 * n).min(63)))
            .unwrap_or(9)
    };
    let swapped: Vec<_> = clean
        .windows(2)
        .filter(|w| w[0].2 != w[1].2 && vlen(w[0].2) == vlen(w[1].2))
        .take(1)
        .collect();
    assert_eq!(swapped.len(), 1);
    let (e0, e1) = (&swapped[0][0], &swapped[0][1]);
    eprintln!(
        "pair e0={} {} {} e1={} {} {}",
        e0.0,
        hex(&e0.1),
        e0.2,
        e1.0,
        hex(&e1.1),
        e1.2
    );
    for f in &found {
        let names_pair = (f.detail.contains(&hex(&e0.1)) && f.detail.contains(&hex(&e1.1)))
            && (f.detail.contains(&format!("offset {}", e0.2))
                || f.detail.contains(&format!("offset {}", e1.2)));
        assert!(
            names_pair,
            "finding must name both keys and an offset: {}",
            f.detail
        );
    }
    // Header-only bound: Σ(2 + key length) over every entry, never a partition body.
    let expected: u64 = clean.iter().map(|e| 2 + e.1.len() as u64).sum();
    assert_eq!(
        header_bytes, expected,
        "invariant 1 must read exactly the partition headers"
    );
    assert!(
        header_bytes < 20_000,
        "bound is tiny next to the 600+ KiB Data.db"
    );
    // Isolation: no OTHER row fails and the structural checks stay clean.
    assert!(
        r.findings
            .iter()
            .all(|f| f.class == VerifyErrorClass::IndexPositionKeyMismatch),
        "{}",
        describe(&r)
    );
}

#[tokio::test]
async fn a2_2_corrupted_summary_sample_position_is_caught() {
    let clean = clean_index_entries(&component(
        &clean_dir("test_basic", "simple_table"),
        "-Index.db",
    ));
    let r = audit_fixture("summary_sample_position_corrupt", false).await;
    assert_eq!(
        verdict(&r, "summary-index-correlation"),
        AuditVerdict::Fail,
        "{}",
        describe(&r)
    );
    let f = only_class(&r, VerifyErrorClass::SummarySampleMismatch);
    assert_eq!(f.len(), 1, "{}", describe(&r));
    // sample 1 claims the key of Index entry 128; the corrupted position is entry 1's start.
    assert!(f[0].detail.contains("sample 1 "), "{}", f[0].detail);
    assert!(
        f[0].detail.contains(&hex(&clean[128].1)),
        "claimed key: {}",
        f[0].detail
    );
    assert!(
        f[0].detail.contains(&hex(&clean[1].1)),
        "key found at the corrupt position: {}",
        f[0].detail
    );
    assert_eq!(verdict(&r, "index-data-identity"), AuditVerdict::Pass);
    assert!(r
        .findings
        .iter()
        .all(|f| f.class == VerifyErrorClass::SummarySampleMismatch));
}

#[tokio::test]
async fn a3_3_bti_bloom_false_negative_is_caught() {
    let r = audit_fixture("filter_db_bit_flip_bti", false).await;
    assert_eq!(
        verdict(&r, "bloom-no-false-negative"),
        AuditVerdict::Fail,
        "{}",
        describe(&r)
    );
    assert_eq!(
        only_class(&r, VerifyErrorClass::FilterFalseNegative).len(),
        1,
        "{}",
        describe(&r)
    );
    assert_eq!(verdict(&r, "bti-trie-bounds"), AuditVerdict::Pass);
    assert!(r
        .findings
        .iter()
        .all(|f| f.class == VerifyErrorClass::FilterFalseNegative));
}

#[tokio::test]
async fn a4_3_out_of_bounds_data_offset_leaf_is_caught_without_a_scan() {
    let r = audit_fixture("bti_data_offset_out_of_bounds", false).await;
    assert_eq!(
        verdict(&r, "bti-trie-bounds"),
        AuditVerdict::Fail,
        "{}",
        describe(&r)
    );
    let f = only_class(&r, VerifyErrorClass::BtiDataOffsetOutOfBounds);
    assert_eq!(f.len(), 1, "{}", describe(&r));
    assert!(f[0].detail.contains("position 127"), "{}", f[0].detail);
    // A shallow audit never scanned.
    assert!(r.rows_scanned.is_none());
}

#[tokio::test]
async fn a7_2_short_chunk_table_is_caught() {
    let r = audit_fixture("compression_info_short_coverage", false).await;
    assert_eq!(
        verdict(&r, "compression-coverage"),
        AuditVerdict::Fail,
        "{}",
        describe(&r)
    );
    let f = only_class(&r, VerifyErrorClass::CompressionCoverageGap);
    assert_eq!(f.len(), 1, "{}", describe(&r));
    // lz4_table: 12 chunks; data_length raised by one chunk_length implies 13.
    assert!(
        f[0].detail.contains("declares 12") && f[0].detail.contains("implies 13"),
        "{}",
        f[0].detail
    );
}

#[tokio::test]
async fn a8_statistics_partition_count_mismatch_needs_deep_and_is_caught() {
    let shallow = audit_fixture("statistics_partition_count_mismatch", false).await;
    assert_eq!(
        verdict(&shallow, "statistics-vs-scan"),
        AuditVerdict::Skip("requires --deep".into())
    );
    assert!(shallow.findings.is_empty(), "{}", describe(&shallow));
    let deep = audit_fixture("statistics_partition_count_mismatch", true).await;
    assert_eq!(
        verdict(&deep, "statistics-vs-scan"),
        AuditVerdict::Fail,
        "{}",
        describe(&deep)
    );
    let f = only_class(&deep, VerifyErrorClass::StatisticsScanMismatch);
    assert_eq!(f.len(), 1);
    assert!(
        f[0].detail.contains("declares 2") && f[0].detail.contains("observed 1"),
        "{}",
        f[0].detail
    );
}

// ---- reused / existing fixtures ------------------------------------------------

#[tokio::test]
async fn a3_2_existing_big_bloom_fixture_fails_with_the_existing_class() {
    let r = audit_fixture("filter_db_bit_flip", false).await;
    assert_eq!(
        verdict(&r, "bloom-no-false-negative"),
        AuditVerdict::Fail,
        "{}",
        describe(&r)
    );
    assert_eq!(
        only_class(&r, VerifyErrorClass::FilterFalseNegative).len(),
        1
    );
}

#[tokio::test]
async fn a4_2_existing_bti_footer_flip_fails_the_trie_row() {
    let r = audit_fixture("bti_partitions_footer_flip", false).await;
    assert_eq!(
        verdict(&r, "bti-trie-bounds"),
        AuditVerdict::Fail,
        "{}",
        describe(&r)
    );
    assert!(
        !only_class(&r, VerifyErrorClass::BtiRootPointerCorrupt).is_empty(),
        "{}",
        describe(&r)
    );
    assert!(!r.is_ok());
}

const BOUNDARY: &str = "boundary source unreadable";

#[tokio::test]
async fn a5_1_toc_row_reflects_the_existing_missing_component_finding() {
    let r = audit_fixture("toc_missing_component", false).await;
    assert_eq!(
        verdict(&r, "toc-exact-listing"),
        AuditVerdict::Fail,
        "{}",
        describe(&r)
    );
    assert!(!only_class(&r, VerifyErrorClass::MissingComponent).is_empty());
}

#[tokio::test]
async fn a6_1_digest_row_reflects_the_existing_digest_mismatch() {
    let r = audit_fixture("digest_crc32_mismatch", false).await;
    assert_eq!(
        verdict(&r, "digest-matches-data"),
        AuditVerdict::Fail,
        "{}",
        describe(&r)
    );
    assert!(!only_class(&r, VerifyErrorClass::DigestMismatch).is_empty());
}

#[tokio::test]
async fn a9_1_corrupt_big_index_skips_both_dependent_rows() {
    let r = audit_fixture("index_db_bit_flip_big", false).await;
    assert_eq!(
        verdict(&r, "index-data-identity"),
        AuditVerdict::Skip(BOUNDARY.into()),
        "{}",
        describe(&r)
    );
    assert_eq!(
        verdict(&r, "summary-index-correlation"),
        AuditVerdict::Skip(BOUNDARY.into())
    );
    assert_eq!(
        verdict(&r, "bloom-no-false-negative"),
        AuditVerdict::Skip(BOUNDARY.into())
    );
    assert!(
        !only_class(&r, VerifyErrorClass::IndexEntryCorrupt).is_empty(),
        "{}",
        describe(&r)
    );
}

#[tokio::test]
async fn a9_2_corrupt_bti_boundary_skips_trie_and_bloom_rows() {
    let r = audit_fixture("bti_rows_truncation", false).await;
    assert_eq!(
        verdict(&r, "bti-trie-bounds"),
        AuditVerdict::Skip(BOUNDARY.into()),
        "{}",
        describe(&r)
    );
    assert_eq!(
        verdict(&r, "bloom-no-false-negative"),
        AuditVerdict::Skip(BOUNDARY.into())
    );
    assert!(
        !only_class(&r, VerifyErrorClass::BtiTrieCorrupt).is_empty(),
        "{}",
        describe(&r)
    );
}

// ---- Skip / absence / shape ----------------------------------------------------

#[tokio::test]
async fn a2_3_missing_summary_skips_naming_the_cause() {
    let tmp = tempfile::tempdir().expect("tmp");
    let src = clean_dir("test_basic", "simple_table");
    for e in std::fs::read_dir(&src).expect("src") {
        let e = e.expect("entry");
        let n = e.file_name().to_string_lossy().to_string();
        if n.ends_with("-Summary.db")
            || n.ends_with(".jsonl")
            || n.ends_with(".txt") && !n.ends_with("TOC.txt")
        {
            continue;
        }
        std::fs::copy(e.path(), tmp.path().join(&n)).expect("copy");
    }
    // TOC still lists Summary.db => MissingComponent, which is correct; the row is the question.
    let r = audit_generation(&data_db(tmp.path()), false).await;
    assert_eq!(
        verdict(&r, "summary-index-correlation"),
        AuditVerdict::Skip("no Summary.db".into()),
        "{}",
        describe(&r)
    );
}

#[tokio::test]
async fn a0_audit_is_always_eight_rows_in_fixed_order_and_none_for_other_modes() {
    let dir = clean_dir("test_comp", "lz4_table");
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    let audit = verify_sstable_audit(&dir, false, &config, platform.clone())
        .await
        .expect("audit");
    let ids: Vec<_> = audit
        .audit
        .as_ref()
        .expect("audit")
        .rows
        .iter()
        .map(|r| r.invariant)
        .collect();
    assert_eq!(
        ids,
        [
            "index-data-identity",
            "summary-index-correlation",
            "bloom-no-false-negative",
            "bti-trie-bounds",
            "toc-exact-listing",
            "digest-matches-data",
            "compression-coverage",
            "statistics-vs-scan"
        ]
    );
    // A8.1 / A7: shallow audit of the healthy compressed BIG fixture.
    assert_eq!(
        verdict(&audit, "statistics-vs-scan"),
        AuditVerdict::Skip("requires --deep".into())
    );
    assert_eq!(verdict(&audit, "compression-coverage"), AuditVerdict::Pass);
    assert!(audit.is_ok(), "{}", describe(&audit));
    for mode in [VerifyMode::Quick, VerifyMode::Full] {
        let r = verify_sstable(&dir, mode, &config, platform.clone())
            .await
            .expect("verify");
        assert!(r.audit.is_none(), "{mode:?} must not carry an audit");
    }
}

#[tokio::test]
async fn a7_3_uncompressed_fixture_skips_coverage_and_a10_2_mixed_pass_skip_is_ok() {
    let r = audit_generation(
        &data_db(&clean_dir("test_comp", "uncompressed_table")),
        false,
    )
    .await;
    assert_eq!(
        verdict(&r, "compression-coverage"),
        AuditVerdict::Skip("no CompressionInfo: uncompressed".into())
    );
    assert!(r.is_ok(), "{}", describe(&r));
}

#[tokio::test]
async fn a10_1_a_vacuous_audit_is_not_ok_and_names_zero_measured() {
    let tmp = tempfile::tempdir().expect("tmp");
    let src = data_db(&clean_dir("test_comp", "uncompressed_table"));
    std::fs::copy(&src, tmp.path().join("nb-1-big-Data.db")).expect("copy");
    std::fs::write(tmp.path().join("nb-1-big-TOC.txt"), "Data.db\nTOC.txt\n").expect("toc");
    let r = audit_generation(&tmp.path().join("nb-1-big-Data.db"), false).await;
    let a = r.audit.as_ref().expect("audit");
    assert!(a.vacuous(), "{}", describe(&r));
    assert_eq!(a.measured(), 0);
    assert!(!r.is_ok(), "a vacuous audit must not read as clean");
}

/// Task 4.2: what the PRE-EXISTING checks say about each new fixture. A fixture
/// that trips an old check is not isolating its invariant, so the exact set of
/// classes `--mode quick` / `--mode full` report is pinned per fixture.
///
/// `bti_data_offset_out_of_bounds` is the one documented exception under Full:
/// the existing scan-based identity check also fails it ("not a decoded
/// partition start") — exactly the gap A4 describes: the old check only fires
/// if and when a FULL scan runs. Audit catches it without any scan (A4.3).
#[tokio::test]
async fn new_fixtures_are_invisible_to_existing_checks_except_the_documented_one() {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    for name in NEW_FIXTURES {
        let dir = fixture(name);
        for mode in [VerifyMode::Quick, VerifyMode::Full] {
            let r = verify_sstable(&dir, mode, &config, platform.clone())
                .await
                .expect("verify");
            let mut classes: Vec<_> = r.findings.iter().map(|f| f.class.code()).collect();
            classes.sort();
            classes.dedup();
            let expected: &[&str] =
                if name == "bti_data_offset_out_of_bounds" && mode == VerifyMode::Full {
                    &["BtiRootPointerCorrupt"]
                } else {
                    &[]
                };
            assert_eq!(classes, expected, "{name} under {mode:?}: {}", describe(&r));
        }
    }
}
