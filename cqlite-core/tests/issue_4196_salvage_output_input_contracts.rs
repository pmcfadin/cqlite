//! Issue #4196 — two salvage CONTRACTS that nothing pinned (C-audit findings).
//!
//! Both are properties the implementation already has; neither had a test, and
//! an unpinned property is one refactor away from being lost silently.
//!
//! # 1. The #1406 write-surface boundary: no `CompressionInfo.db`, ever
//!
//! CLAUDE.md's claim boundary (`claim.blocked.compressed_sstable_writes`) says
//! CQLite's production write surface emits UNCOMPRESSED SSTables and never a
//! `CompressionInfo.db`; design D4 restates it for salvage, and `salvage --help`
//! tells the operator so ([`cqlite-cli`'s `Commands::Salvage` `long_about`],
//! pinned by `salvage_cli_tests::help_states_uncompressed_whole_partition_and_rebuild_boundaries`).
//! The enforcement is STRUCTURAL — `SalvageOptions {}` has no compression field
//! and there is no code path that could configure one — so
//! [`salvaged_output_never_contains_a_compression_info_db`] is expected to pass
//! today. That is the point: the assertion is what makes it a CONTRACT rather
//! than an accident of the current struct shape. It is asserted over a
//! COMPRESSED input (LZ4 BIG and LZ4 BTI), which is the only input class for
//! which "the output is uncompressed" says anything at all.
//!
//! # 2. Spec R5.1: the input is not modified
//!
//! Salvage is a recovery tool pointed at data its operator has already lost once.
//! Its input must survive the run byte-identically so a second, better attempt is
//! still possible. Production salvage contains no write primitive aimed at the
//! input, so [`salvage_never_modifies_a_byte_of_its_input`] is also expected to
//! pass — it pins the read-only contract by DIGEST (SHA-256 of every file in the
//! input generation directory, before and after), which is the only form of the
//! claim that a future change cannot quietly falsify.
//!
//! # Why the input is a COPY
//!
//! Each case copies its committed fixture into a temp directory and salvages the
//! COPY. The assertion is identical (same bytes, different path), but a test that
//! salvaged the shared corpus in place would, in the exact failure it exists to
//! detect, CORRUPT the corpus every other lane depends on.
//!
//! # Fixture doctrine (issue #3220)
//!
//! Both fixtures are git-tracked (`git ls-files` shows their `-Data.db` binaries
//! committed), so their absence is a fail-closed FAILURE, not a skip — asserted
//! PER CASE, never with a suite-wide `ran > 0`, which cannot see one case
//! skipping behind its siblings.

// `not(tombstones)`: mirrors `write_engine::salvage`'s own module gate — the
// decode-at-offset primitive is compiled out with `tombstones` on, so this
// target must compile out identically or the `tombstones`-on lanes fail on an
// unresolved import.
#![cfg(all(feature = "write-support", not(feature = "tombstones")))]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use cqlite_core::schema::TableSchema;
use cqlite_core::storage::write_engine::salvage::{salvage_sstable, SalvageOptions, SalvageReport};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

#[path = "support/datasets_root.rs"]
mod datasets_root;

/// A fixture whose `-Data.db` is COMMITTED to git, and which is LZ4-compressed
/// on disk (verified at run time by the presence of a `*-CompressionInfo.db` in
/// the input, so the #1406 assertion can never be vacuous).
struct CompressedCommittedFixture {
    keyspace: &'static str,
    table: &'static str,
    schema_file: &'static str,
    /// What this fixture is, for the diagnostics.
    what: &'static str,
}

/// The two committed, compressed fixtures both contracts are asserted over —
/// one per on-disk format family, since the output component SET differs
/// (`nb`: Index.db + Summary.db + CRC.db; `da`: Partitions.db + Rows.db) and a
/// boundary about which components are written must be checked on both.
const COMPRESSED_COMMITTED_FIXTURES: &[CompressedCommittedFixture] = &[
    CompressedCommittedFixture {
        keyspace: "test_comp",
        table: "lz4_table",
        schema_file: "compression-parity.cql",
        what: "BIG (`nb`) LZ4-compressed",
    },
    CompressedCommittedFixture {
        keyspace: "test_da",
        table: "multiclustering_table",
        schema_file: "multiclustering-table-bti.cql",
        what: "BTI (`da`) LZ4-compressed",
    },
];

/// Load one `CREATE TABLE` out of a committed CQL schema fixture.
fn table_schema(schema_file: &str, table: &str, keyspace: &str) -> TableSchema {
    let schema_path = datasets_root::schema_path(schema_file)
        .unwrap_or_else(|| panic!("committed CQL schema {schema_file} must resolve"));
    let cql = std::fs::read_to_string(&schema_path)
        .unwrap_or_else(|e| panic!("read {schema_path:?}: {e}"));
    let start = cql
        .find(&format!("CREATE TABLE IF NOT EXISTS {table}"))
        .or_else(|| cql.find(&format!("CREATE TABLE {table}")))
        .unwrap_or_else(|| panic!("{schema_path:?} declares no table {table}"));
    let end = start
        + cql[start..]
            .find(';')
            .unwrap_or_else(|| panic!("{schema_path:?}: unterminated CREATE TABLE {table}"))
        + 1;
    let mut schema = cqlite_core::schema::cql_parser::parse_cql_schema(&cql[start..end])
        .unwrap_or_else(|e| panic!("parse CREATE TABLE {table}: {e}"));
    schema.keyspace = keyspace.to_string();
    schema
}

/// The ONE generation directory of a COMMITTED fixture, or a hard failure
/// naming every root searched.
///
/// Fail-closed UNCONDITIONALLY — not gated on `CQLITE_REQUIRE_FIXTURES` (issue
/// #3220): the binaries these cases need are committed to this repository, so
/// "absent" means the checkout is broken, never that a dataset was not fetched.
fn committed_generation_dir(fixture: &CompressedCommittedFixture) -> PathBuf {
    datasets_root::resolve_table_generation_dir(fixture.keyspace, fixture.table).unwrap_or_else(
        |searched| {
            panic!(
                "COMMITTED fixture {}.{} ({}) is absent — its *-Data.db is git-tracked, so this \
                 is a broken checkout, NOT an unfetched dataset, and must never skip. {searched}",
                fixture.keyspace, fixture.table, fixture.what
            )
        },
    )
}

fn single_data_db(dir: &Path) -> PathBuf {
    let mut found: Vec<PathBuf> = Vec::new();
    for e in std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("read {dir:?}: {e}"))
        .flatten()
    {
        if e.file_name().to_string_lossy().ends_with("-Data.db") {
            found.push(e.path());
        }
    }
    match found.len() {
        1 => found.remove(0),
        n => panic!("{dir:?}: expected exactly ONE Data.db, found {n} ({found:?})"),
    }
}

/// Every file directly under `dir`, copied into `dest` (which is created).
///
/// Flat by design: a Cassandra generation directory holds only files.
fn copy_generation_dir(dir: &Path, dest: &Path) {
    std::fs::create_dir_all(dest).unwrap_or_else(|e| panic!("create {dest:?}: {e}"));
    let mut copied = 0usize;
    for entry in std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("read {dir:?}: {e}"))
        .flatten()
    {
        let from = entry.path();
        if from.is_dir() {
            panic!("{from:?}: a generation directory must hold files only");
        }
        std::fs::copy(&from, dest.join(entry.file_name()))
            .unwrap_or_else(|e| panic!("copy {from:?}: {e}"));
        copied += 1;
    }
    assert!(copied > 0, "{dir:?} held no files to copy");
}

/// `file name -> lowercase hex SHA-256` for every file directly under `dir`.
///
/// The MAP (not a single rolled-up digest) so a difference can be reported by
/// NAME: "some file changed" is not an actionable failure message for a
/// read-only-contract violation.
fn digest_dir(dir: &Path) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for entry in std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("read {dir:?}: {e}"))
        .flatten()
    {
        let path = entry.path();
        if path.is_dir() {
            panic!("{path:?}: unexpected subdirectory in a generation directory");
        }
        let bytes = std::fs::read(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
        let digest = Sha256::digest(&bytes);
        out.insert(
            entry.file_name().to_string_lossy().to_string(),
            format!("{digest:x}"),
        );
    }
    assert!(!out.is_empty(), "{dir:?}: nothing to digest");
    out
}

/// Every file path anywhere under `dir` (recursively), sorted. Empty when `dir`
/// does not exist.
///
/// RECURSIVE because `SSTableWriter` nests its output at
/// `<out>/<keyspace>/<table>/`: a top-level-only listing of the `--out` root
/// sees the keyspace directory and NO components, so any component assertion
/// made against it passes vacuously.
fn files_under(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(next) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&next) else {
            continue;
        };
        for entry in rd.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else {
                out.push(path);
            }
        }
    }
    out.sort();
    out
}

/// One salvage run over a COPY of a committed fixture: the input's digests
/// before and after, the files written under `--out`, and the report.
struct SalvageRun {
    input_dir: PathBuf,
    digests_before: BTreeMap<String, String>,
    digests_after: BTreeMap<String, String>,
    output_files: Vec<PathBuf>,
    report: SalvageReport,
    /// Kept alive for the lifetime of the run's paths.
    _temp: TempDir,
}

/// Copy `fixture` into a temp dir, digest it, salvage it, digest it again.
///
/// Asserts the two PREMISES both contracts depend on, so neither can pass
/// vacuously: the input really is compressed (a `*-CompressionInfo.db` is
/// present), and the salvage really recovered something (no refusal, no losses,
/// `recovered > 0`).
async fn salvage_copy_of(fixture: &CompressedCommittedFixture) -> SalvageRun {
    let source_dir = committed_generation_dir(fixture);
    let temp = TempDir::new().expect("tempdir");
    let input_dir = temp.path().join("input");
    copy_generation_dir(&source_dir, &input_dir);

    // Premise 1: the input is COMPRESSED. Without this, "the output has no
    // CompressionInfo.db" is trivially true of an uncompressed input too, and
    // the #1406 assertion would prove nothing.
    let compression_info: Vec<String> = std::fs::read_dir(&input_dir)
        .unwrap_or_else(|e| panic!("read {input_dir:?}: {e}"))
        .flatten()
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|n| n.ends_with("-CompressionInfo.db"))
        .collect();
    assert_eq!(
        compression_info.len(),
        1,
        "{}.{} ({}) must carry exactly one *-CompressionInfo.db for these assertions to mean \
         anything; found {compression_info:?} in {input_dir:?}",
        fixture.keyspace,
        fixture.table,
        fixture.what
    );

    let schema = table_schema(fixture.schema_file, fixture.table, fixture.keyspace);
    let data_db = single_data_db(&input_dir);
    let digests_before = digest_dir(&input_dir);

    let out_root = temp.path().join("out");
    let report = salvage_sstable(&data_db, &out_root, &schema, SalvageOptions::default())
        .await
        .unwrap_or_else(|e| {
            panic!(
                "{}.{}: salvage of a healthy committed fixture must succeed: {e}",
                fixture.keyspace, fixture.table
            )
        });

    // Premise 2: the run actually recovered data.
    assert!(
        report.refused.is_none() && report.losses.is_empty(),
        "{}.{}: healthy fixture salvage must neither refuse nor lose anything; report={report:?}",
        fixture.keyspace,
        fixture.table
    );
    assert!(
        report.partitions.recovered > 0,
        "{}.{}: salvage recovered ZERO partitions — every assertion below would be vacuous; \
         report={report:?}",
        fixture.keyspace,
        fixture.table
    );

    let digests_after = digest_dir(&input_dir);
    let output_files = files_under(&out_root);
    SalvageRun {
        input_dir,
        digests_before,
        digests_after,
        output_files,
        report,
        _temp: temp,
    }
}

/// Issue #1406 (CLAUDE.md's write-surface claim boundary) — a salvaged output
/// directory contains NO `*-CompressionInfo.db`, anywhere under `--out`, for a
/// COMPRESSED input of either format family.
///
/// The C-audit on this issue found no test asserting this at all: the boundary
/// was documented (design D4), promised to the operator in `salvage --help`, and
/// structurally enforced (`SalvageOptions {}` carries no compression knob) — but
/// unasserted, so a later change that plumbed compression through would break a
/// published claim with a green suite.
#[tokio::test]
async fn salvaged_output_never_contains_a_compression_info_db() {
    for fixture in COMPRESSED_COMMITTED_FIXTURES {
        let run = salvage_copy_of(fixture).await;

        // Non-vacuity: a real generation was written, so "no CompressionInfo.db"
        // is a statement about a populated output and not about an empty dir.
        let data_dbs: Vec<&PathBuf> = run
            .output_files
            .iter()
            .filter(|p| {
                p.file_name()
                    .map(|n| n.to_string_lossy().ends_with("-Data.db"))
                    .unwrap_or(false)
            })
            .collect();
        assert_eq!(
            data_dbs.len(),
            1,
            "{}.{}: expected exactly ONE Data.db under --out; got {data_dbs:?} (all files: {:?})",
            fixture.keyspace,
            fixture.table,
            run.output_files
        );

        let compression_info: Vec<&PathBuf> = run
            .output_files
            .iter()
            .filter(|p| {
                p.file_name()
                    .map(|n| n.to_string_lossy().ends_with("-CompressionInfo.db"))
                    .unwrap_or(false)
            })
            .collect();
        assert!(
            compression_info.is_empty(),
            "{}.{} ({}): salvage wrote {compression_info:?} — CQLite's production write surface \
             emits UNCOMPRESSED SSTables and never a CompressionInfo.db (issue #1406, design D4, \
             and what `salvage --help` promises the operator). The input WAS compressed, so this \
             is a real boundary violation, not a fixture artifact.",
            fixture.keyspace,
            fixture.table,
            fixture.what
        );
        // The manifest must also record that the INPUT was compressed — a
        // `false` here would mean the compressed-input premise above was
        // measured by this test and not by the code under test.
        assert!(
            run.report.compressed_input,
            "{}.{}: the input carries a CompressionInfo.db, so the manifest must report \
             compressed_input: true; report={:?}",
            fixture.keyspace, fixture.table, run.report
        );
        eprintln!(
            "[issue_4196] {}.{} ({}): compressed input -> {} output file(s), 0 CompressionInfo.db \
             (#1406).",
            fixture.keyspace,
            fixture.table,
            fixture.what,
            run.output_files.len()
        );
    }
}

/// Spec R5.1 — salvage does not modify its input. Every file in the input
/// generation directory has the SAME SHA-256 after the run as before it, and the
/// file SET is unchanged (nothing added, nothing removed).
///
/// The C-audit on this issue found R5.1 asserted nowhere. Production salvage
/// contains no write primitive aimed at the input, so this passes today; the
/// digest comparison is what keeps it true. It matters more than a
/// "should-already-hold" test usually does: salvage runs on data the operator
/// has ALREADY lost once, and an input mutated by the recovery attempt takes the
/// second, better attempt with it.
#[tokio::test]
async fn salvage_never_modifies_a_byte_of_its_input() {
    for fixture in COMPRESSED_COMMITTED_FIXTURES {
        let run = salvage_copy_of(fixture).await;
        let subject = format!("{}.{}", fixture.keyspace, fixture.table);

        if let Some(removed) = run
            .digests_before
            .keys()
            .find(|name| !run.digests_after.contains_key(*name))
        {
            panic!(
                "{subject}: salvage REMOVED input component {removed:?} from {:?} — the input \
                 must survive a salvage run intact (spec R5.1)",
                run.input_dir
            );
        }
        if let Some(added) = run
            .digests_after
            .keys()
            .find(|name| !run.digests_before.contains_key(*name))
        {
            panic!(
                "{subject}: salvage CREATED {added:?} inside the INPUT directory {:?} — salvage \
                 writes only under --out (spec R5.1)",
                run.input_dir
            );
        }
        if let Some((name, before)) = run
            .digests_before
            .iter()
            .find(|(name, before)| run.digests_after.get(*name) != Some(*before))
        {
            panic!(
                "{subject}: salvage MODIFIED input component {name:?}:\n  before: {before}\n  \
                 after:  {:?}\nThe input must be byte-identical after a salvage run (spec R5.1) \
                 — salvage runs on data its operator has already lost once.",
                run.digests_after.get(name)
            );
        }
        assert_eq!(
            run.digests_before, run.digests_after,
            "{subject}: input digests differ after salvage (spec R5.1)"
        );
        eprintln!(
            "[issue_4196] {subject}: {} input component(s) SHA-256-unchanged across a salvage run \
             that recovered {} partition(s) (spec R5.1).",
            run.digests_before.len(),
            run.report.partitions.recovered
        );
    }
}
