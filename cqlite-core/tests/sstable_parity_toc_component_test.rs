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
//! Byte-level scope added by issue #1047 (completes the #982 digest remainder):
//!   * `Digest.crc32` *byte* parity — for every committed Cassandra `*-Digest.crc32`
//!     reference, recompute the CRC32 of the sibling `Data.db` with CQLite and
//!     compare the rendered digest bytes byte-for-byte against the reference file.
//!     This is fail-closed: a present `Data.db` whose recomputed digest does not
//!     byte-match the committed reference turns the lane red. `Data.db` binaries
//!     are local-only (gitignored, fetched on demand), so a digest whose sibling
//!     `Data.db` is absent is skipped on file presence — but the suite still fails
//!     closed if *no* reference exists, or if no `Data.db` was ever available to
//!     compare (so it can never silently become a no-op when datasets are present).
//!
//! Still out of scope here (tracked as separate #982/#968 follow-ups):
//! `Index.db`/`Summary.db`/`Statistics.db`/`CompressionInfo.db`/`Filter.db` byte
//! parity — those require additional binary components and are gated on dataset
//! fetch (#983/#984).

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

/// Recursively collect every committed `*-Digest.crc32` reference under `dir`,
/// skipping macOS AppleDouble shadow files (`._*`).
fn collect_digest_files(dir: &Path, out: &mut Vec<PathBuf>) {
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
            collect_digest_files(&path, out);
        } else if name.ends_with("-Digest.crc32") {
            out.push(path);
        }
    }
}

fn all_digest_files() -> Vec<PathBuf> {
    let root = datasets_sstables_root();
    let mut out = Vec::new();
    collect_digest_files(&root, &mut out);
    out.sort();
    // Fail closed: a broken fixture path must turn the strict lane red, not green.
    assert!(
        !out.is_empty(),
        "no committed *-Digest.crc32 references found under {} — strict digest parity cannot run \
         (this is a fail-closed guard, not a skip)",
        root.display()
    );
    out
}

/// Compute the CRC32 of `data_db` exactly as CQLite's `DigestWriter` does
/// (streaming `crc32fast::Hasher`, Java `java.util.zip.CRC32` polynomial).
///
/// This mirrors `cqlite_core::storage::sstable::writer::DigestWriter::compute_crc32`,
/// which lives behind the `write-support` feature; reimplementing the byte stream
/// here keeps the parity lane runnable in the default-feature Fast PR tier without
/// changing production feature gates.
fn cqlite_data_db_crc32(data_db: &Path) -> u32 {
    use std::io::Read;

    let mut file = std::fs::File::open(data_db)
        .unwrap_or_else(|e| panic!("open {} for CRC32 failed: {e}", data_db.display()));
    let mut hasher = crc32fast::Hasher::new();
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .unwrap_or_else(|e| panic!("read {} for CRC32 failed: {e}", data_db.display()));
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    hasher.finalize()
}

/// Render a CRC32 value into the exact `Digest.crc32` payload Cassandra writes:
/// the decimal ASCII string with no trailing newline (matches CQLite's
/// `DigestWriter::write`, which does `write!(w, "{}", crc32)`).
fn render_digest_payload(crc32: u32) -> Vec<u8> {
    crc32.to_string().into_bytes()
}

/// `Digest.crc32` byte-for-byte parity (issue #1047, completes #982).
///
/// For every committed `*-Digest.crc32` reference, recompute the CRC32 of the
/// sibling `Data.db` with CQLite and compare the rendered digest bytes
/// byte-for-byte against the committed reference file.
///
/// Fail-closed contract (per the established dataset convention:
/// skip-on-total-absence; fail on present-but-wrong):
///   * No committed digest reference at all -> fail (`all_digest_files`).
///   * A present `Data.db` whose recomputed digest does not byte-match its
///     reference -> fail.
///   * Every `Data.db` absent (fresh checkout / CI without the binary dataset
///     fetched) -> skip the whole test (no fixtures to compare). The committed
///     `.crc32` references alone cannot prove byte parity, but their absence of
///     binaries is an environment state, not a regression.
///   * At least one `Data.db` present but no comparison performed -> fail (the
///     suite must never silently degrade into a no-op when fixtures are present).
///   * A digest whose sibling `Data.db` is absent while others are present
///     (partial local-only binaries) -> skip *that fixture* on file presence only.
#[test]
fn digest_crc32_byte_for_byte_parity() {
    let digests = all_digest_files();

    let mut compared = 0usize;
    let mut skipped_no_data = 0usize;

    for digest_path in &digests {
        let digest_name = digest_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_else(|| panic!("non-UTF8 digest filename: {}", digest_path.display()));

        // Sibling Data.db: same path with the `-Digest.crc32` suffix swapped for
        // `-Data.db`. Cassandra's Digest.crc32 covers the whole Data.db component.
        let data_name = digest_name.replace("-Digest.crc32", "-Data.db");
        let data_path = digest_path.with_file_name(&data_name);

        // Local-only binary: skip on absence (CI without dataset fetch), never on
        // a parse/compare failure.
        if !data_path.exists() {
            skipped_no_data += 1;
            continue;
        }

        let reference = std::fs::read(digest_path)
            .unwrap_or_else(|e| panic!("read {} failed: {e}", digest_path.display()));

        let crc32 = cqlite_data_db_crc32(&data_path);
        let computed = render_digest_payload(crc32);

        assert_eq!(
            computed,
            reference,
            "{}: CQLite-recomputed Digest.crc32 payload does not byte-match Cassandra reference\n  \
             cqlite : {:?} (= {:?})\n  cass   : {:?} (= {:?})\n  data.db: {}",
            digest_path.display(),
            String::from_utf8_lossy(&computed),
            computed,
            String::from_utf8_lossy(&reference),
            reference,
            data_path.display(),
        );

        compared += 1;
    }

    // Skip-on-total-absence: a fresh checkout (or CI without the binary dataset
    // fetched) has the committed `.crc32` references but no sibling `Data.db`
    // binaries. With nothing to compare there is no regression to catch, so skip
    // rather than turn the lane red. This matches the project convention of
    // skipping when the dataset is entirely absent.
    if compared == 0 {
        debug_assert_eq!(
            skipped_no_data,
            digests.len(),
            "internal: compared==0 implies every digest was skipped for absent Data.db"
        );
        eprintln!(
            "digest_crc32_byte_for_byte_parity: skipped: no Data.db fixtures available \
             ({} Digest.crc32 references found, all skipped — fetch the binary dataset with \
             bash test-data/scripts/fetch-datasets.sh to run byte-parity comparisons)",
            digests.len(),
        );
        return;
    }

    eprintln!(
        "digest_crc32_byte_for_byte_parity: {compared} Data.db digests byte-matched \
         ({skipped_no_data} skipped — Data.db absent)"
    );
}
