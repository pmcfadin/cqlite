//! Issue #4612: a truncated or corrupt `Statistics.db` must fail the READER closed, naming the
//! path — never degrade to a partial parse whose EncodingStats / SerializationHeader baselines
//! are wrong (swapped columns, every write timestamp re-based to 2015).
//!
//! Oracle: Cassandra-written generations from datasets-v4 (`test_basic.static_columns_table`,
//! BIG `nb`; `test_da.simple_table`, BTI `da`), damaged by truncation at several lengths and by
//! flipping single bytes. The REGRESSION guard proves the defect was real by showing the old
//! lenient parser still accepts at least one of the damaged files; the readers must refuse it.
//! Tools that REPORT on damage (`diagnose`, `verify --audit`, `salvage`) must turn the same
//! failure into their documented outcome, never abort or give a confident wrong answer.
//! Every case asserts independently; there is no suite-wide `ran > 0`.

#[path = "support/datasets_root.rs"]
mod datasets_root;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use cqlite_core::parser::enhanced_statistics_parser::parse_statistics_with_fallback;
use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::storage::sstable::verify::{verify_sstable_generation_audit, VerifyErrorClass};
use cqlite_core::storage::write_engine::merge::compact_sstables;
use cqlite_core::storage::write_engine::salvage::{salvage_sstable, SalvageOptions};
use cqlite_core::Config;
use tempfile::TempDir;

const BIG: (&str, &str) = ("test_basic", "static_columns_table");
const BTI: (&str, &str) = ("test_da", "simple_table");

/// A copy of one Cassandra-written generation in a temp dir, plus its `Statistics.db` bytes.
struct Copy {
    _tmp: TempDir,
    dir: PathBuf,
    data: PathBuf,
    stats: PathBuf,
    pristine: Vec<u8>,
}

fn require_fixtures_strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

fn stage((keyspace, table): (&str, &str)) -> Option<Copy> {
    let Some(root) = datasets_root::sstables_root_for_table(keyspace, table) else {
        assert!(
            !require_fixtures_strict(),
            "CQLITE_REQUIRE_FIXTURES=1 but {keyspace}.{table} is absent; {}",
            datasets_root::describe_search(keyspace, table)
        );
        eprintln!("[issue_4612] {keyspace}.{table} absent; skipping");
        return None;
    };
    let src = datasets_root::table_generation_dirs(&root, keyspace, table)
        .into_iter()
        .next()
        .expect("a generation directory");
    let tmp = TempDir::new().expect("tempdir");
    let dir = tmp.path().to_path_buf();
    let mut data = None;
    let mut stats = None;
    for entry in std::fs::read_dir(&src).expect("read fixture dir").flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        std::fs::copy(&path, dir.join(&name)).expect("copy component");
        if name.ends_with("-Data.db") {
            data = Some(dir.join(&name));
        }
        if name.ends_with("-Statistics.db") {
            stats = Some(dir.join(&name));
        }
    }
    let stats = stats.expect("fixture has a Statistics.db");
    let pristine = std::fs::read(&stats).expect("read Statistics.db");
    Some(Copy {
        _tmp: tmp,
        dir,
        data: data.expect("fixture has a Data.db"),
        stats,
        pristine,
    })
}

/// Truncations at several lengths and one-byte flips across the file (TOC, body, trailing CRC).
fn damaged_variants(pristine: &[u8]) -> Vec<(String, Vec<u8>)> {
    let n = pristine.len();
    let mut out = Vec::new();
    for len in [0, 3, 4, 8, 12, 40, n / 4, n / 2, n - 40, n - 5, n - 4, n - 1] {
        out.push((format!("truncated to {len} of {n} bytes"), pristine[..len].to_vec()));
    }
    for at in [0, 5, 9, 17, n / 3, n / 2, 2 * n / 3, n - 6, n - 1] {
        let mut b = pristine.to_vec();
        b[at] ^= 0x01;
        out.push((format!("byte {at} of {n} flipped"), b));
    }
    out
}

async fn platform() -> (Config, Arc<Platform>) {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    (config, platform)
}

fn assert_named(err: &str, stats: &Path, label: &str) {
    assert!(
        err.contains(&stats.display().to_string()) && err.contains("Statistics.db"),
        "{label}: the error must name the Statistics.db path {}; got: {err}",
        stats.display()
    );
}

async fn open_err(data: &Path) -> Option<String> {
    let (config, platform) = platform().await;
    SSTableReader::open(data, &config, platform)
        .await
        .err()
        .map(|e| e.to_string())
}

/// Open and SELECT (`scan`) fail with a named error for every damaged variant, on BIG and BTI.
#[tokio::test]
async fn open_and_scan_refuse_every_damaged_statistics_db() {
    for fixture in [BIG, BTI] {
        let Some(copy) = stage(fixture) else { continue };
        // Control: the pristine copy opens, so a refusal below is caused by the damage alone.
        let (config, platform) = platform().await;
        SSTableReader::open(&copy.data, &config, platform)
            .await
            .unwrap_or_else(|e| panic!("{fixture:?}: pristine generation must open: {e}"));
        for (label, bytes) in damaged_variants(&copy.pristine) {
            std::fs::write(&copy.stats, &bytes).expect("write damaged Statistics.db");
            let label = format!("{fixture:?} {label}");
            let err = open_err(&copy.data)
                .await
                .unwrap_or_else(|| panic!("{label}: open must fail closed, it returned Ok"));
            assert_named(&err, &copy.stats, &label);
        }
    }
}

/// The silent-degradation case itself: at least one damaged file is STILL accepted by the old
/// lenient parser (so the defect was real), and the reader now refuses every such file instead of
/// opening with partial EncodingStats / SerializationHeader state.
#[tokio::test]
async fn files_the_lenient_parser_accepts_no_longer_open() {
    for fixture in [BIG, BTI] {
        let Some(copy) = stage(fixture) else { continue };
        let (_, pristine_stats) = parse_statistics_with_fallback(&copy.pristine, None)
            .expect("pristine Statistics.db parses");
        let mut lenient_accepted = 0;
        for (label, bytes) in damaged_variants(&copy.pristine) {
            let Ok((_, stats)) = parse_statistics_with_fallback(&bytes, None) else {
                continue;
            };
            lenient_accepted += 1;
            // What the old open would have used: a parse that differs from the real file's.
            let differs = stats.timestamp_stats.min_timestamp
                != pristine_stats.timestamp_stats.min_timestamp
                || stats.serialization_header_columns.len()
                    != pristine_stats.serialization_header_columns.len()
                || stats.serialization_header_columns.iter().map(|c| &c.name).ne(pristine_stats
                    .serialization_header_columns
                    .iter()
                    .map(|c| &c.name));
            std::fs::write(&copy.stats, &bytes).expect("write damaged Statistics.db");
            let label = format!("{fixture:?} {label} (lenient parse differs: {differs})");
            let err = open_err(&copy.data)
                .await
                .unwrap_or_else(|| panic!("{label}: lenient-accepted file must not open"));
            assert_named(&err, &copy.stats, &label);
        }
        assert!(
            lenient_accepted > 0,
            "{fixture:?}: no damaged variant parsed leniently, so this guard cannot show the old defect"
        );
    }
}

/// A damaged input is a named error from compaction — never an `Ok` with a re-based baseline.
#[tokio::test]
async fn compaction_refuses_a_damaged_input_naming_the_path() {
    let Some(copy) = stage(BIG) else { return };
    let schema = {
        let p = datasets_root::schema_path("basic-types.cql").expect("committed schema");
        let cql = std::fs::read_to_string(p).expect("schema");
        let start = cql
            .find(&format!("CREATE TABLE IF NOT EXISTS {}", BIG.1))
            .expect("create table");
        let end = start + cql[start..].find(';').unwrap() + 1;
        let mut t = cqlite_core::schema::cql_parser::parse_cql_schema(&cql[start..end]).unwrap();
        t.keyspace = BIG.0.to_string();
        t
    };
    for (label, bytes) in damaged_variants(&copy.pristine) {
        std::fs::write(&copy.stats, &bytes).expect("write damaged Statistics.db");
        let out = TempDir::new().expect("out");
        let err = compact_sstables(vec![copy.data.clone()], out.path(), &schema, 1, None, None, false)
            .await
            .err()
            .unwrap_or_else(|| panic!("{label}: compaction must refuse, it returned Ok"))
            .to_string();
        assert_named(&err, &copy.stats, &label);
    }
}

/// `verify --audit` reports a named finding (never `Err`, never all-clear).
#[tokio::test]
async fn audit_reports_a_named_finding_for_a_damaged_statistics_db() {
    let Some(copy) = stage(BIG) else { return };
    let (config, platform) = platform().await;
    let clean = verify_sstable_generation_audit(&copy.data, false, &config, platform.clone())
        .await
        .expect("audit of the pristine copy");
    assert!(clean.findings.is_empty(), "control: {:?}", clean.findings);
    for (label, bytes) in damaged_variants(&copy.pristine) {
        std::fs::write(&copy.stats, &bytes).expect("write damaged Statistics.db");
        let report = verify_sstable_generation_audit(&copy.data, false, &config, platform.clone())
            .await
            .unwrap_or_else(|e| panic!("{label}: audit must report, not abort: {e}"));
        assert!(!report.is_ok(), "{label}: a damaged Statistics.db must not audit clean");
        assert!(
            report
                .findings
                .iter()
                .any(|f| f.class == VerifyErrorClass::StatisticsHeaderCorrupt),
            "{label}: expected a StatisticsHeaderCorrupt finding, got {:?}",
            report.findings
        );
    }
}

/// `diagnose` lists the generation under `unreadable_generations` with the path in the cause.
#[tokio::test]
async fn diagnose_names_the_generation_as_unreadable() {
    use cqlite_core::storage::sstable::diagnose::{diagnose_table, DiagnoseOptions};
    let Some(copy) = stage(BIG) else { return };
    let options = DiagnoseOptions {
        now_secs: 1_700_000_000,
        deep: false,
        top_n: 5,
        gc_grace_seconds: None,
        schema: None,
    };
    let clean = diagnose_table(&copy.dir, &options).await.expect("pristine diagnose");
    assert!(clean.unreadable_generations.is_empty() && clean.generations.len() == 1);
    for (label, bytes) in damaged_variants(&copy.pristine) {
        std::fs::write(&copy.stats, &bytes).expect("write damaged Statistics.db");
        let report = diagnose_table(&copy.dir, &options)
            .await
            .unwrap_or_else(|e| panic!("{label}: diagnose must report, not abort: {e}"));
        assert!(report.generations.is_empty(), "{label}: no values from a damaged file");
        let gen = report
            .unreadable_generations
            .first()
            .unwrap_or_else(|| panic!("{label}: the generation must be listed unreadable"));
        assert_eq!(gen.data_path, copy.data, "{label}");
    }
}

/// `salvage` keeps its documented contract: a classified refusal recorded in the manifest, never
/// a hard `Err` and never a recovery computed from a partial header.
#[tokio::test]
async fn salvage_refuses_with_a_classified_manifest() {
    let Some(copy) = stage(BIG) else { return };
    let schema = {
        let p = datasets_root::schema_path("basic-types.cql").expect("committed schema");
        let cql = std::fs::read_to_string(p).expect("schema");
        let start = cql
            .find(&format!("CREATE TABLE IF NOT EXISTS {}", BIG.1))
            .expect("create table");
        let end = start + cql[start..].find(';').unwrap() + 1;
        let mut t = cqlite_core::schema::cql_parser::parse_cql_schema(&cql[start..end]).unwrap();
        t.keyspace = BIG.0.to_string();
        t
    };
    for (label, bytes) in damaged_variants(&copy.pristine) {
        std::fs::write(&copy.stats, &bytes).expect("write damaged Statistics.db");
        let out = TempDir::new().expect("out");
        let report = salvage_sstable(&copy.data, out.path(), &schema, SalvageOptions::default())
            .await
            .unwrap_or_else(|e| panic!("{label}: salvage must record a refusal, not abort: {e}"));
        let refusal = report
            .refused
            .unwrap_or_else(|| panic!("{label}: salvage must refuse a damaged Statistics.db"));
        let text = format!("{refusal:?}");
        assert!(text.contains("Statistics.db"), "{label}: refusal must name the component: {text}");
        assert_eq!(report.partitions.written, 0, "{label}: nothing may be written");
    }
}
