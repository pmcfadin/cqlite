//! Issue #4626 phase 4 (task 4.1; GD1 to GD3): `rebuild_components` takes its generation and its
//! Statistics trust from the generation module.
//!
//! Oracles: Cassandra 5.0.8-written fixtures (`test-data/fixtures/issue_4626` and the committed
//! `test_basic` corpus), copied to a tempdir before any mutation. The bytes a rebuild must
//! reproduce (`Digest.crc32`) are compared with the Cassandra-written original, never with a
//! second CQLite run. Each resolver panics by name when its fixture is missing (#3220).

#![cfg(all(feature = "write-support", not(feature = "tombstones")))]

#[path = "support/datasets_root.rs"]
mod datasets_root;
#[path = "support/generation_fixtures.rs"]
mod generation_fixtures;

use std::path::{Path, PathBuf};

use cqlite_core::schema::TableSchema;
use cqlite_core::storage::write_engine::rebuild::{
    rebuild_components, Component, RebuildOptions, RebuildReport,
};
use cqlite_core::Error;
use generation_fixtures::*;
use tempfile::TempDir;

fn gd_schema(table: &str) -> TableSchema {
    cqlite_core::schema::cql_parser::parse_cql_schema(&format!(
        "CREATE TABLE gd4626.{table} (id int PRIMARY KEY, v text)"
    ))
    .expect("schema")
}

fn basic_schema() -> TableSchema {
    let p = datasets_root::schema_path("basic-types.cql").expect("committed schema");
    let cql = std::fs::read_to_string(p).expect("schema");
    let start = cql
        .find("CREATE TABLE IF NOT EXISTS static_columns_table")
        .expect("create table");
    let end = start + cql[start..].find(';').unwrap() + 1;
    let mut t = cqlite_core::schema::cql_parser::parse_cql_schema(&cql[start..end]).unwrap();
    t.keyspace = "test_basic".to_string();
    t
}

fn options(out: &Path) -> RebuildOptions {
    RebuildOptions {
        out_dir: out.to_path_buf(),
        statistics_recovery_source: None,
    }
}

async fn rebuild(
    data: &Path,
    schema: &TableSchema,
    components: &[Component],
    out: &Path,
) -> Result<RebuildReport, Error> {
    rebuild_components(data, schema, components, &options(out)).await
}

fn names(dir: &Path) -> Vec<String> {
    let mut v: Vec<String> = std::fs::read_dir(dir)
        .map(|rd| {
            rd.flatten()
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default();
    v.sort();
    v
}

/// GD1.1: a `ma-1-big` descriptor with no `Statistics.db` is `UnsupportedVersion` naming `ma` and
/// `na`, nothing is written (not even `--out`), and no classification exists to carry `lost`.
#[tokio::test]
async fn gd1_1_below_floor_generation_is_refused_before_any_open() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().join("in");
    touch(&dir.join("ma-1-big-Data.db"));
    let out = tmp.path().join("out");
    let result = rebuild(
        &dir.join("ma-1-big-Data.db"),
        &gd_schema("t"),
        &[Component::Statistics],
        &out,
    )
    .await;
    match result {
        Err(Error::UnsupportedVersion { version, floor }) => {
            assert_eq!((version.as_str(), floor.as_str()), ("ma", "na"));
        }
        other => panic!("expected UnsupportedVersion, got {other:?}"),
    }
    assert!(
        !out.exists(),
        "a refusal must write nothing, not even --out"
    );
}

/// GD2.1: a committed `test_basic` generation with `TOC.txt` deleted is rebuilt; the report names
/// the input condition and the regenerated `TOC.txt` lists `Data.db`.
#[tokio::test]
async fn gd2_1_toc_missing_generation_is_rebuilt_and_recorded() {
    let src = corpus_table("test_basic", "static_columns_table");
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().join("in");
    copy_dir(&src, &dir);
    let toc = std::fs::read_dir(&dir)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .find(|p| p.to_string_lossy().ends_with("-TOC.txt"))
        .expect("fixture has a TOC.txt");
    let toc_name = toc.file_name().unwrap().to_string_lossy().into_owned();
    std::fs::remove_file(&toc).unwrap();
    let data = std::fs::read_dir(&dir)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .find(|p| p.to_string_lossy().ends_with("-Data.db"))
        .unwrap();
    let out = tmp.path().join("out");
    let report = rebuild(&data, &basic_schema(), &[Component::Toc], &out)
        .await
        .expect("a TocMissing generation is rebuilt");
    assert!(report.refused.is_none(), "{:?}", report.refused);
    assert_eq!(report.input_conditions, vec!["TocMissing".to_string()]);
    let toc_text = std::fs::read_to_string(out.join(&toc_name)).expect("TOC.txt regenerated");
    assert!(toc_text.lines().any(|l| l == "Data.db"), "{toc_text}");
    // The original (Cassandra-written) TOC is the oracle for the component set it lists.
    let original = std::fs::read_to_string(src.join(&toc_name)).unwrap();
    let mut want: Vec<&str> = original.lines().filter(|l| !l.is_empty()).collect();
    let mut got: Vec<&str> = toc_text.lines().filter(|l| !l.is_empty()).collect();
    want.sort();
    got.sort();
    assert_eq!(got, want);
}

/// GD2.2 on the Cassandra-written UUID corpus: the regenerated `Digest.crc32` is byte-identical
/// to Cassandra's, every output file carries the id text verbatim, and `crc` is not applicable to
/// compressed input.
#[tokio::test]
async fn gd2_2_uuid_id_generation_is_rebuilt_with_its_id_text() {
    let src = fixture_table("uuid_ids", "uuid_only");
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().join("in");
    copy_dir(&src, &dir);
    let base = "nb-3h5t_0i6t_1jigq2lxu6305iz8wl-big";
    let out = tmp.path().join("out");
    let report = rebuild(
        &dir.join(format!("{base}-Data.db")),
        &gd_schema("uuid_only"),
        &[Component::Digest, Component::Crc, Component::Toc],
        &out,
    )
    .await
    .expect("a UUID-id generation is rebuilt");
    assert!(report.refused.is_none(), "{:?}", report.refused);
    assert!(report.input_conditions.is_empty(), "{report:?}");
    assert_eq!(
        std::fs::read(out.join(format!("{base}-Digest.crc32"))).unwrap(),
        std::fs::read(src.join(format!("{base}-Digest.crc32"))).unwrap(),
        "Digest.crc32 must equal the Cassandra-written original"
    );
    assert!(
        report
            .skipped_not_applicable
            .iter()
            .any(|s| s.component == "crc"),
        "{report:?}"
    );
    for name in names(&out) {
        assert!(name.starts_with(base), "{name} lost the id text");
    }
    assert!(out.join(format!("{base}-Data.db")).is_file());
}

/// GD2.3: an incomplete transaction log whose old files are gone refuses the run, naming the log,
/// and writes nothing.
#[tokio::test]
async fn gd2_3_txn_inconsistent_directory_is_refused_naming_the_log() {
    let src = fixture_table("txn_inconsistent", "txn_compaction");
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().join("gd4626").join(src.file_name().unwrap());
    copy_dir(&src, &dir);
    let out = tmp.path().join("out");
    let err = rebuild(
        &dir.join("nb-4-big-Data.db"),
        &gd_schema("txn_compaction"),
        &[Component::Digest],
        &out,
    )
    .await
    .expect_err("an inconsistent directory is refused");
    let text = err.to_string();
    assert!(text.contains("nb_txn_compaction_"), "{text}");
    assert!(!out.exists(), "nothing may be written");
}

/// An explicitly named file that a committed transaction log hides (an obsoleted compaction
/// input) is refused naming the log, exactly as the table-dir listing excludes it.
#[tokio::test]
async fn a_file_hidden_by_a_committed_log_is_refused_naming_the_log() {
    let (_root, dir) = stamped_fixture_copy("txn_committed");
    let tmp = TempDir::new().unwrap();
    let out = tmp.path().join("out");
    let err = rebuild(
        &dir.join("nb-1-big-Data.db"),
        &gd_schema("txn_compaction"),
        &[Component::Digest],
        &out,
    )
    .await
    .expect_err("a hidden generation is not rebuilt");
    assert!(err.to_string().contains("nb_txn_compaction_"), "{err}");
    assert!(!out.exists());
}

/// An Orphan (zero-length `Data.db`) is reported, not rebuilt.
#[tokio::test]
async fn an_orphan_is_refused_not_rebuilt() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().join("in");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("nb-1-big-Data.db"), b"").unwrap();
    let out = tmp.path().join("out");
    let err = rebuild(
        &dir.join("nb-1-big-Data.db"),
        &gd_schema("t"),
        &[Component::Digest],
        &out,
    )
    .await
    .expect_err("an orphan is not rebuilt");
    assert!(err.to_string().contains("Orphan"), "{err}");
    assert!(!out.exists());
}

fn flip_byte(path: &Path) {
    let mut bytes = std::fs::read(path).unwrap();
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0xff;
    std::fs::write(path, bytes).unwrap();
}

/// GD3.1 on a UUID-id Cassandra generation: a flipped byte in the sibling `Statistics.db` still
/// fails closed, naming the path and both remedies.
#[tokio::test]
async fn gd3_1_corrupt_statistics_still_fails_closed_naming_both_remedies() {
    let src = fixture_table("uuid_ids", "uuid_only");
    let tmp = TempDir::new().unwrap();
    let dir: PathBuf = tmp.path().join("in");
    copy_dir(&src, &dir);
    let base = "nb-3h5t_0i6t_1jigq2lxu6305iz8wl-big";
    let stats = dir.join(format!("{base}-Statistics.db"));
    flip_byte(&stats);
    let out = tmp.path().join("out");
    let err = rebuild(
        &dir.join(format!("{base}-Data.db")),
        &gd_schema("uuid_only"),
        &[Component::Index],
        &out,
    )
    .await
    .expect_err("a damaged sibling fails closed")
    .to_string();
    assert!(err.contains(&format!("{base}-Statistics.db")), "{err}");
    assert!(
        err.contains("rename the damaged Statistics.db aside"),
        "{err}"
    );
    assert!(err.contains("statistics_recovery_source"), "{err}");
    assert!(!out.exists());
}
