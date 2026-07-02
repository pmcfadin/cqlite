//! Coverage analysis: how much of the Cassandra high-relevance corpus is
//! classified in the manifest. High-relevance gaps are errors under `--strict`,
//! warnings otherwise (medium/low always warn) — per issue #976.
//!
//! Matching is **path-precise** (issue #1199). The Cassandra test corpus contains
//! distinct files that share a basename at different source paths *and* different
//! relevance levels — e.g. `VersionSupportedFeaturesTest.java` exists as a 🔴 High
//! file under `io/sstable/format/big/` and as a 🟡 Med file under
//! `io/sstable/format/bti/`, and `SerializationsTest.java` exists as High
//! (`utils/`), Med (`service/`), and Low (`gms/`). Basename-only matching is
//! therefore unsound: a manifest entry referencing the Med/Low `SerializationsTest`
//! by basename would falsely "classify" the High one. We instead key the
//! high-relevance set by full source path and only let a basename-only manifest
//! entry classify a high file when that basename is *globally unambiguous* across
//! the entire index; an ambiguous high file must be classified by its full path.

use std::collections::{BTreeMap, BTreeSet};

use crate::model::Manifest;

pub struct CoverageReport {
    pub high_total: usize,
    pub high_classified: usize,
    /// Full source paths of high-relevance files no manifest scenario classifies.
    pub unclassified_high: Vec<String>,
}

/// A single high-relevance Cassandra test file: its full source path (relative to
/// the Cassandra source root) and its basename.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct HighFile {
    pub path: String,
    pub basename: String,
}

/// The path-precise high-relevance corpus plus the basename ambiguity map needed
/// to decide whether a basename-only manifest reference may classify a file.
pub struct IndexCorpus {
    /// 🔴 High files, keyed by full path.
    pub high: Vec<HighFile>,
    /// basename -> set of *all* full paths (any relevance) carrying that basename.
    /// A basename with more than one path is ambiguous and may not be classified
    /// by a basename-only manifest reference.
    pub basename_paths: BTreeMap<String, BTreeSet<String>>,
}

/// Parse the Cassandra test index into the path-precise high-relevance corpus.
///
/// The high-relevance set is taken from the detailed per-file sections (each a
/// `#### 🔴 High · \`Name.java\`` header followed by a `- **Path:** \`...\`` line),
/// NOT the basename-only "quick list" table — only the detailed sections carry the
/// disambiguating full path. The basename->paths map spans every relevance level
/// so we can detect cross-relevance basename collisions.
pub fn parse_index(index_text: &str) -> IndexCorpus {
    let mut high: BTreeSet<HighFile> = BTreeSet::new();
    let mut basename_paths: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();

    let mut pending_high = false;
    let mut pending_any = false;
    for line in index_text.lines() {
        let trimmed = line.trim_start();
        if let Some(relevance) = detail_header_relevance(trimmed) {
            pending_high = relevance == Relevance::High;
            pending_any = true;
            continue;
        }
        if pending_any {
            if let Some(path) = path_line_value(trimmed) {
                let basename = normalized_basename(&path).to_string();
                basename_paths
                    .entry(basename.clone())
                    .or_default()
                    .insert(path.clone());
                if pending_high {
                    high.insert(HighFile { path, basename });
                }
                pending_high = false;
                pending_any = false;
            }
        }
    }

    IndexCorpus {
        high: high.into_iter().collect(),
        basename_paths,
    }
}

#[derive(PartialEq, Eq)]
enum Relevance {
    High,
    Other,
}

/// Detect a detailed-section header `#### 🔴 High · \`Name.java\`` (or 🟡 Med / ⚪ Low)
/// and return its relevance. Returns `None` for any other line.
fn detail_header_relevance(line: &str) -> Option<Relevance> {
    let rest = line.strip_prefix("#### ")?;
    // The relevance word follows the emoji + space; match on the word to avoid
    // depending on exact emoji bytes.
    if rest.contains("High") && rest.contains('·') {
        Some(Relevance::High)
    } else if (rest.contains("Med") || rest.contains("Low")) && rest.contains('·') {
        Some(Relevance::Other)
    } else {
        None
    }
}

/// Extract the path from a `- **Path:** \`...\`` line.
fn path_line_value(line: &str) -> Option<String> {
    let rest = line.strip_prefix("- **Path:**")?;
    first_backtick_token(rest)
}

fn first_backtick_token(line: &str) -> Option<String> {
    let start = line.find('`')? + 1;
    let rest = &line[start..];
    let end = rest.find('`')?;
    Some(rest[..end].to_string())
}

/// Normalize a manifest/index file reference down to its basename. Used to detect
/// whether a manifest reference is path-qualified and to key the ambiguity map.
pub fn normalized_basename(path: &str) -> &str {
    let path = path.trim();
    match path.rsplit(['/', '\\']).next() {
        Some(name) => name,
        None => path,
    }
}

/// True if the manifest reference carries a path (contains a separator), i.e. it
/// is more than a bare basename.
fn is_path_qualified(reference: &str) -> bool {
    let r = reference.trim();
    r.contains('/') || r.contains('\\')
}

/// True if a `cassandra.files` reference is a **test-kind** reference by the
/// documented manifest file-kind naming convention (issue #1408): its basename
/// ends in `Test.java`. Production sources (e.g. `BigTableWriter.java`), the
/// `CQLTester.java` harness, category `#anchor` references, and the `n/a`
/// placeholder are NOT test-kind and are therefore exempt from index resolution.
pub fn is_test_reference(reference: &str) -> bool {
    !reference.contains('#') && normalized_basename(reference).ends_with("Test.java")
}

/// True if a test-kind reference resolves to a detailed entry in the Cassandra
/// test index (issue #1408): its basename carries at least one `**Path:**` entry,
/// and — when the reference is itself path-qualified — that exact path is among
/// the indexed paths for the basename (so a path-qualified reference never
/// resolves against a same-basename twin at a different path).
pub fn test_reference_resolves(corpus: &IndexCorpus, reference: &str) -> bool {
    let base = normalized_basename(reference);
    match corpus.basename_paths.get(base) {
        None => false,
        Some(paths) => {
            if is_path_qualified(reference) {
                paths.contains(&normalize_path(reference))
            } else {
                true
            }
        }
    }
}

/// Normalize a reference to a comparable full-path key (trim only); path
/// separators are left intact so distinct paths never collapse.
fn normalize_path(reference: &str) -> String {
    reference.trim().replace('\\', "/")
}

/// Compute coverage of high-relevance files against the manifest's referenced
/// Cassandra files, **path-precise** (issue #1199).
///
/// A high-relevance file (keyed by full path) is classified iff some manifest
/// `cassandra.files` entry either
///  - matches it by full path (path-qualified reference equal to the high path), or
///  - matches it by basename *and that basename is globally unambiguous* in the
///    index (exactly one source path carries it). An ambiguous basename
///    (shared with a Med/Low or other-path twin) can only classify via full path,
///    so a basename-only reference never falsely classifies the wrong file.
pub fn analyze(m: &Manifest, index_text: &str) -> CoverageReport {
    let corpus = parse_index(index_text);

    // Collect manifest references split into full-path keys and basename keys.
    let mut manifest_paths: BTreeSet<String> = BTreeSet::new();
    let mut manifest_basenames: BTreeSet<String> = BTreeSet::new();
    for s in &m.scenarios {
        for f in &s.cassandra.files {
            if is_path_qualified(f) {
                manifest_paths.insert(normalize_path(f));
            } else {
                manifest_basenames.insert(normalized_basename(f).to_string());
            }
        }
    }

    let mut classified = 0usize;
    let mut unclassified = Vec::new();
    for hf in &corpus.high {
        let by_path = manifest_paths.contains(&normalize_path(&hf.path));
        // A basename-only manifest reference may only classify a globally
        // unambiguous basename. `>= 1` because the high file itself contributes
        // one path; `> 1` means a twin exists at another path (ambiguous).
        let basename_unambiguous = corpus
            .basename_paths
            .get(&hf.basename)
            .map(|paths| paths.len() == 1)
            .unwrap_or(true);
        let by_basename = basename_unambiguous && manifest_basenames.contains(&hf.basename);

        if by_path || by_basename {
            classified += 1;
        } else {
            unclassified.push(hf.path.clone());
        }
    }

    CoverageReport {
        high_total: corpus.high.len(),
        high_classified: classified,
        unclassified_high: unclassified,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Manifest;

    /// A manifest whose only Cassandra file references are the *longer* names that
    /// a substring match would wrongly accept as classifying the shorter
    /// high-relevance files. Exact (path-precise) matching must leave the shorter
    /// names unclassified.
    const SUBSTRING_TRAP_MANIFEST: &str = r#"
manifest_version: 1
cassandra_source:
  repo: r
  ref: r
  sha: s
  index: docs/cassandra_test_index.md
  assessment_report: a
program:
  parent_epic: 1
  reporting_epic: 1
scenarios:
  - id: cass.repair.pending
    title: t
    status: out_of_scope
    capability: c
    priority: P2
    risk: node_behavior
    cassandra:
      category: repair
      relevance: high
      files:
        - test/unit/org/apache/cassandra/db/repair/PendingAntiCompactionTest.java
    cqlite: {}
    evidence:
      type: out_of_scope
    ci:
      tier: manual_debug
  - id: cass.tombstone.repaired
    title: t
    status: out_of_scope
    capability: c
    priority: P2
    risk: node_behavior
    cassandra:
      category: tombstone-ttl
      relevance: high
      files:
        - RepairedDataTombstonesTest.java
    cqlite: {}
    evidence:
      type: out_of_scope
    ci:
      tier: manual_debug
"#;

    const SUBSTRING_TRAP_INDEX: &str = "\
#### 🔴 High · `AntiCompactionTest.java`
- **Path:** `test/unit/org/apache/cassandra/db/compaction/AntiCompactionTest.java`
#### 🔴 High · `PendingAntiCompactionTest.java`
- **Path:** `test/unit/org/apache/cassandra/db/repair/PendingAntiCompactionTest.java`
#### 🔴 High · `TombstonesTest.java`
- **Path:** `test/unit/org/apache/cassandra/db/TombstonesTest.java`
#### 🔴 High · `RepairedDataTombstonesTest.java`
- **Path:** `test/unit/org/apache/cassandra/db/RepairedDataTombstonesTest.java`
";

    fn unclassified_basenames(cov: &CoverageReport) -> Vec<String> {
        cov.unclassified_high
            .iter()
            .map(|p| normalized_basename(p).to_string())
            .collect()
    }

    #[test]
    fn normalized_basename_strips_paths() {
        assert_eq!(
            normalized_basename("test/unit/org/apache/cassandra/db/AntiCompactionTest.java"),
            "AntiCompactionTest.java"
        );
        assert_eq!(
            normalized_basename(" TombstonesTest.java "),
            "TombstonesTest.java"
        );
    }

    /// Regression for issue #1199: matching must NOT let the longer
    /// `PendingAntiCompactionTest.java` classify `AntiCompactionTest.java`,
    /// nor `RepairedDataTombstonesTest.java` classify `TombstonesTest.java`.
    #[test]
    fn exact_match_distinguishes_substring_collisions() {
        let m = Manifest::from_yaml(SUBSTRING_TRAP_MANIFEST).unwrap();
        let cov = analyze(&m, SUBSTRING_TRAP_INDEX);
        let unclassified = unclassified_basenames(&cov);

        // The two longer names ARE classified by their own scenarios.
        assert!(!unclassified.contains(&"PendingAntiCompactionTest.java".to_string()));
        assert!(!unclassified.contains(&"RepairedDataTombstonesTest.java".to_string()));

        // The two shorter names must remain UNclassified — substring matching
        // would have falsely classified them.
        assert!(
            unclassified.contains(&"AntiCompactionTest.java".to_string()),
            "AntiCompactionTest.java must NOT be classified by PendingAntiCompactionTest.java"
        );
        assert!(
            unclassified.contains(&"TombstonesTest.java".to_string()),
            "TombstonesTest.java must NOT be classified by RepairedDataTombstonesTest.java"
        );
    }

    /// Two distinct files sharing a basename at different paths and different
    /// relevance levels must be classified independently (issue #1199 deeper
    /// soundness bug). A manifest entry that classifies the Med-relevance
    /// `format/bti/VersionSupportedFeaturesTest.java` by *basename* must NOT
    /// thereby classify the High-relevance `format/big/...` twin — the High twin
    /// is only satisfied by its full path.
    const DUP_BASENAME_INDEX: &str = "\
#### 🔴 High · `VersionSupportedFeaturesTest.java`
- **Path:** `test/unit/org/apache/cassandra/io/sstable/format/big/VersionSupportedFeaturesTest.java`
#### 🟡 Med · `VersionSupportedFeaturesTest.java`
- **Path:** `test/unit/org/apache/cassandra/io/sstable/format/bti/VersionSupportedFeaturesTest.java`
";

    const DUP_BASENAME_ONLY_MANIFEST: &str = r#"
manifest_version: 1
cassandra_source:
  repo: r
  ref: r
  sha: s
  index: docs/cassandra_test_index.md
  assessment_report: a
program:
  parent_epic: 1
  reporting_epic: 1
scenarios:
  - id: cass.x.basename_only
    title: t
    status: out_of_scope
    capability: c
    priority: P2
    risk: node_behavior
    cassandra:
      category: sstable-format
      relevance: high
      files:
        - VersionSupportedFeaturesTest.java
    cqlite: {}
    evidence:
      type: out_of_scope
    ci:
      tier: manual_debug
"#;

    const DUP_BASENAME_PATH_MANIFEST: &str = r#"
manifest_version: 1
cassandra_source:
  repo: r
  ref: r
  sha: s
  index: docs/cassandra_test_index.md
  assessment_report: a
program:
  parent_epic: 1
  reporting_epic: 1
scenarios:
  - id: cass.x.path_precise
    title: t
    status: out_of_scope
    capability: c
    priority: P2
    risk: node_behavior
    cassandra:
      category: sstable-format
      relevance: high
      files:
        - test/unit/org/apache/cassandra/io/sstable/format/big/VersionSupportedFeaturesTest.java
    cqlite: {}
    evidence:
      type: out_of_scope
    ci:
      tier: manual_debug
"#;

    #[test]
    fn same_basename_different_path_classified_independently() {
        // A basename-only reference must NOT satisfy the High twin, because the
        // basename is ambiguous (shared with the Med twin at a different path).
        let m = Manifest::from_yaml(DUP_BASENAME_ONLY_MANIFEST).unwrap();
        let cov = analyze(&m, DUP_BASENAME_INDEX);
        assert_eq!(cov.high_total, 1, "only the High file is high-relevance");
        assert_eq!(
            cov.high_classified, 0,
            "basename-only reference must not classify an ambiguous high file"
        );
        assert_eq!(
            cov.unclassified_high,
            vec![
                "test/unit/org/apache/cassandra/io/sstable/format/big/VersionSupportedFeaturesTest.java"
                    .to_string()
            ]
        );

        // The full path DOES classify the correct twin.
        let mp = Manifest::from_yaml(DUP_BASENAME_PATH_MANIFEST).unwrap();
        let covp = analyze(&mp, DUP_BASENAME_INDEX);
        assert_eq!(covp.high_classified, 1);
        assert!(
            covp.unclassified_high.is_empty(),
            "full-path reference classifies the High twin"
        );
    }

    // ---- issue #1408: file-kind convention + test-ref index resolution ----

    #[test]
    fn is_test_reference_recognizes_only_test_basenames() {
        // test-kind: basename ends in Test.java
        assert!(is_test_reference("CellTest.java"));
        assert!(is_test_reference(
            "test/unit/org/apache/cassandra/db/CellTest.java"
        ));
        // source: production .java not ending in Test.java
        assert!(!is_test_reference("BigTableWriter.java"));
        assert!(!is_test_reference("DeletionTime.java"));
        // harness / anchor / placeholder
        assert!(!is_test_reference("CQLTester.java"));
        assert!(!is_test_reference("#compaction"));
        assert!(!is_test_reference("n/a"));
        // an anchor whose slug happens to contain the word is still not test-kind
        assert!(!is_test_reference("cassandra_test_index.md#CellTest.java"));
    }

    const RESOLVE_INDEX: &str = "\
#### 🔴 High · `CellTest.java`
- **Path:** `test/unit/org/apache/cassandra/db/CellTest.java`
#### 🟡 Med · `VersionSupportedFeaturesTest.java`
- **Path:** `test/unit/org/apache/cassandra/io/sstable/format/bti/VersionSupportedFeaturesTest.java`
#### 🔴 High · `VersionSupportedFeaturesTest.java`
- **Path:** `test/unit/org/apache/cassandra/io/sstable/format/big/VersionSupportedFeaturesTest.java`
";

    #[test]
    fn test_reference_resolves_by_basename_and_path() {
        let corpus = parse_index(RESOLVE_INDEX);
        // basename-only ref to a uniquely-named indexed test resolves.
        assert!(test_reference_resolves(&corpus, "CellTest.java"));
        // a *Test.java with no index entry does not resolve.
        assert!(!test_reference_resolves(&corpus, "NoSuchPhantomTest.java"));
        // ambiguous basename: basename-only still "resolves" (some entry exists),
        // but a path-qualified ref must match an exact indexed path.
        assert!(test_reference_resolves(
            &corpus,
            "VersionSupportedFeaturesTest.java"
        ));
        assert!(test_reference_resolves(
            &corpus,
            "test/unit/org/apache/cassandra/io/sstable/format/big/VersionSupportedFeaturesTest.java"
        ));
        // path-qualified ref to a path NOT in the index does not resolve, even
        // though the basename exists at other paths.
        assert!(!test_reference_resolves(
            &corpus,
            "test/unit/org/apache/cassandra/io/sstable/format/nowhere/VersionSupportedFeaturesTest.java"
        ));
    }
}
