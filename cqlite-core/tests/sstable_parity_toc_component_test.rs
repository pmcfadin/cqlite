//! Strict TOC.txt + component-manifest parity (Epic #968 / issue #982).
//!
//! Proves CQLite's component model against the **exact** `TOC.txt` manifests that
//! Apache Cassandra 5.0 wrote for every committed fixture — BIG (`nb`/`oa`) and
//! BTI (`da`). These reference files are committed text, so the suite needs no
//! `Data.db` binary and no live Cassandra: it runs in the Fast PR tier.
//!
//! Scope owned here (issue #982):
//!   * Strict `TOC.txt` parsing parity — every component name Cassandra wrote is
//!     recognized by CQLite and round-trips byte-exactly (no silent drops, no
//!     unknown-component fallbacks).
//!   * Component-set completeness — the components every Cassandra SSTable must
//!     carry are present in the manifest.
//!   * BIG-vs-BTI discovery parity — the format declared in the filename
//!     (`big`/`bti`, parsed authoritatively, no heuristics) agrees with the
//!     component manifest Cassandra emitted (`Index.db`+`Summary.db` for BIG,
//!     `Partitions.db`+`Rows.db` for BTI), and the two manifests never mix.
//!
//! Out of scope here (tracked as separate #982/#968 follow-ups): `Digest.crc32`
//! *byte* parity (recomputing the CRC of `Data.db`) and `Index.db`/`Summary.db`/
//! `Statistics.db`/`CompressionInfo.db`/`Filter.db` byte parity — those require
//! the binary components and are gated on dataset fetch.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use cqlite_core::storage::sstable::directory::{parse_toc_file_detailed, SSTableComponent};
use cqlite_core::storage::sstable::version_gate::{SsTableDescriptor, SsTableFormat};

/// Resolve the committed datasets root (env override first, else workspace tree).
fn datasets_sstables_root() -> PathBuf {
    let root = if let Ok(root) = std::env::var("CQLITE_DATASETS_ROOT") {
        PathBuf::from(root)
    } else {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .map(|workspace| workspace.join("test-data/datasets"))
            .unwrap_or_else(|| PathBuf::from("test-data/datasets"))
    };
    root.join("sstables")
}

/// Recursively collect every `*-TOC.txt` reference file under `dir`,
/// skipping macOS AppleDouble shadow files (`._*`).
fn collect_toc_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("._") {
            continue;
        }
        if path.is_dir() {
            collect_toc_files(&path, out);
        } else if name.ends_with("-TOC.txt") {
            out.push(path);
        }
    }
}

fn all_toc_files() -> Vec<PathBuf> {
    let root = datasets_sstables_root();
    let mut out = Vec::new();
    collect_toc_files(&root, &mut out);
    out.sort();
    // Fail closed: a broken fixture path must turn the strict lane red, not green.
    assert!(
        !out.is_empty(),
        "no committed *-TOC.txt fixtures found under {} — strict TOC parity cannot run \
         (this is a fail-closed guard, not a skip)",
        root.display()
    );
    out
}

/// Parse the components named in a TOC, asserting strict recognition and a
/// byte-exact name round-trip. Returns the recognized components.
fn parse_strict(toc: &Path) -> Vec<SSTableComponent> {
    let (components, unknown) = parse_toc_file_detailed(toc)
        .unwrap_or_else(|e| panic!("parse {} failed: {e}", toc.display()));

    assert!(
        unknown.is_empty(),
        "{}: CQLite does not recognize component(s) {:?} that Cassandra wrote into TOC.txt",
        toc.display(),
        unknown,
    );

    // Byte-exact round trip: every recognized component renders back to the
    // exact filename suffix Cassandra used, and the parsed set equals the raw
    // set of lines in the file (nothing silently dropped or remapped).
    let raw: BTreeSet<String> = std::fs::read_to_string(toc)
        .unwrap_or_else(|e| panic!("read {} failed: {e}", toc.display()))
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(str::to_string)
        .collect();
    let parsed: BTreeSet<String> = components
        .iter()
        .map(|c| c.file_extension().to_string())
        .collect();
    assert_eq!(
        parsed,
        raw,
        "{}: parsed component set does not byte-match the raw TOC manifest",
        toc.display(),
    );

    components
}

/// Strict parsing parity + component-set completeness across every fixture.
///
/// Every component name Cassandra wrote must be recognized and round-trip
/// byte-exactly, and the always-present components must be there.
#[test]
fn toc_manifest_strict_parsing_and_completeness() {
    // Components Cassandra writes for every SSTable regardless of format/options.
    // (CompressionInfo.db is intentionally excluded — uncompressed tables omit it.)
    let always_present = [
        SSTableComponent::Data,
        SSTableComponent::Statistics,
        SSTableComponent::TOC,
        SSTableComponent::Digest,
        SSTableComponent::Filter,
    ];

    let tocs = all_toc_files();
    for toc in &tocs {
        let components = parse_strict(toc);
        for required in &always_present {
            assert!(
                components.contains(required),
                "{}: required component {} ({}) missing from Cassandra TOC manifest",
                toc.display(),
                required.file_extension(),
                if required.is_required() {
                    "read-critical"
                } else {
                    "standard"
                },
            );
        }
    }

    eprintln!(
        "toc_manifest_strict_parsing_and_completeness: {} TOC fixtures verified",
        tocs.len()
    );
}

/// BIG-vs-BTI component-manifest discovery parity.
///
/// The format the filename declares (parsed authoritatively from the `<format>`
/// segment — no heuristics) must agree with the component manifest Cassandra
/// emitted, and BIG/BTI component families must never be mixed in one SSTable.
#[test]
fn toc_manifest_big_vs_bti_discovery_parity() {
    let tocs = all_toc_files();

    let mut big_seen = 0usize;
    let mut bti_seen = 0usize;

    for toc in &tocs {
        let filename = toc
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_else(|| panic!("non-UTF8 TOC filename: {}", toc.display()));

        // Authoritative format from the filename's <format> segment.
        let descriptor = SsTableDescriptor::parse_filename(filename)
            .unwrap_or_else(|e| panic!("{}: descriptor parse failed: {e}", toc.display()));

        let components = parse_strict(toc);
        let has_big = components.iter().any(SSTableComponent::is_big_specific);
        let has_bti = components.iter().any(SSTableComponent::is_bti_specific);

        match descriptor.format {
            SsTableFormat::Big => {
                big_seen += 1;
                assert!(
                    has_big && !has_bti,
                    "{}: declared BIG but component manifest is {:?} (expected Index.db+Summary.db, \
                     no Partitions.db/Rows.db)",
                    toc.display(),
                    components,
                );
                assert!(
                    components.contains(&SSTableComponent::Index)
                        && components.contains(&SSTableComponent::Summary),
                    "{}: BIG manifest must carry both Index.db and Summary.db",
                    toc.display(),
                );
            }
            SsTableFormat::Bti => {
                bti_seen += 1;
                assert!(
                    has_bti && !has_big,
                    "{}: declared BTI but component manifest is {:?} (expected Partitions.db+Rows.db, \
                     no Index.db/Summary.db)",
                    toc.display(),
                    components,
                );
                assert!(
                    components.contains(&SSTableComponent::Partitions)
                        && components.contains(&SSTableComponent::Rows),
                    "{}: BTI manifest must carry both Partitions.db and Rows.db",
                    toc.display(),
                );
            }
        }
    }

    // The committed corpus carries both families (nb/oa BIG and da BTI); if either
    // assertion path never executed, the discovery parity claim is unproven.
    assert!(
        big_seen > 0,
        "no BIG-format fixtures exercised — BIG discovery parity unproven"
    );
    assert!(
        bti_seen > 0,
        "no BTI-format fixtures exercised — BTI discovery parity unproven"
    );

    eprintln!(
        "toc_manifest_big_vs_bti_discovery_parity: {big_seen} BIG + {bti_seen} BTI manifests verified"
    );
}
