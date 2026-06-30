//! Coverage analysis: how much of the Cassandra high-relevance corpus is
//! classified in the manifest. High-relevance gaps are errors under `--strict`,
//! warnings otherwise (medium/low always warn) — per issue #976.

use std::collections::BTreeSet;

use crate::model::Manifest;

pub struct CoverageReport {
    pub high_total: usize,
    pub high_classified: usize,
    pub unclassified_high: Vec<String>,
}

/// Extract the high-relevance Cassandra test file names from the index's
/// "High-relevance tests (quick list)" table.
pub fn high_relevance_files(index_text: &str) -> Vec<String> {
    let mut files = BTreeSet::new();
    let mut in_section = false;
    for line in index_text.lines() {
        if line.contains("High-relevance tests (quick list)") {
            in_section = true;
            continue;
        }
        if in_section {
            if line.starts_with("## ") {
                break;
            }
            if line.starts_with("| `") {
                if let Some(name) = first_backtick_token(line) {
                    if name.ends_with(".java") {
                        files.insert(name);
                    }
                }
            }
        }
    }
    files.into_iter().collect()
}

fn first_backtick_token(line: &str) -> Option<String> {
    let start = line.find('`')? + 1;
    let rest = &line[start..];
    let end = rest.find('`')?;
    Some(rest[..end].to_string())
}

/// Normalize a manifest/index file reference down to its basename so a
/// high-relevance file is matched by *exact* filename equality, never by
/// substring. Substring matching is unsound here: `PendingAntiCompactionTest.java`
/// would falsely "classify" `AntiCompactionTest.java`, and
/// `RepairedDataTombstonesTest.java` would falsely "classify" `TombstonesTest.java`,
/// letting `coverage --strict` report 0 unclassified while those files are not
/// actually classified (issue #1199).
pub fn normalized_basename(path: &str) -> &str {
    let path = path.trim();
    match path.rsplit(['/', '\\']).next() {
        Some(name) => name,
        None => path,
    }
}

/// Compute coverage of high-relevance files against the manifest's referenced
/// Cassandra files.
pub fn analyze(m: &Manifest, index_text: &str) -> CoverageReport {
    let high = high_relevance_files(index_text);
    // Index by normalized basename so classification is an *exact* filename
    // match (issue #1199 soundness fix), not a substring match.
    let mut manifest_files: BTreeSet<String> = BTreeSet::new();
    for s in &m.scenarios {
        for f in &s.cassandra.files {
            manifest_files.insert(normalized_basename(f).to_string());
        }
    }

    let mut classified = 0usize;
    let mut unclassified = Vec::new();
    for hf in &high {
        let hit = manifest_files.contains(normalized_basename(hf));
        if hit {
            classified += 1;
        } else {
            unclassified.push(hf.clone());
        }
    }

    CoverageReport {
        high_total: high.len(),
        high_classified: classified,
        unclassified_high: unclassified,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Manifest;

    /// A minimal manifest whose only Cassandra file references are the *longer*
    /// names that a substring match would wrongly accept as classifying the
    /// shorter high-relevance files.
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
## High-relevance tests (quick list)
| `AntiCompactionTest.java` | compaction | x |
| `PendingAntiCompactionTest.java` | repair | x |
| `TombstonesTest.java` | tombstone-ttl | x |
| `RepairedDataTombstonesTest.java` | tombstone-ttl | x |
## Next section
";

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

    /// Regression for issue #1199: exact-basename matching must NOT let the
    /// longer `PendingAntiCompactionTest.java` classify `AntiCompactionTest.java`,
    /// nor `RepairedDataTombstonesTest.java` classify `TombstonesTest.java`.
    /// A substring match would (the old bug); exact matching leaves them
    /// unclassified, which is what makes `coverage --strict` sound.
    #[test]
    fn exact_match_distinguishes_substring_collisions() {
        let m = Manifest::from_yaml(SUBSTRING_TRAP_MANIFEST).unwrap();
        let cov = analyze(&m, SUBSTRING_TRAP_INDEX);

        // The two longer names ARE classified by their own scenarios.
        assert!(!cov
            .unclassified_high
            .iter()
            .any(|f| f == "PendingAntiCompactionTest.java"));
        assert!(!cov
            .unclassified_high
            .iter()
            .any(|f| f == "RepairedDataTombstonesTest.java"));

        // The two shorter names must remain UNclassified — substring matching
        // would have falsely classified them.
        assert!(
            cov.unclassified_high
                .iter()
                .any(|f| f == "AntiCompactionTest.java"),
            "AntiCompactionTest.java must NOT be classified by PendingAntiCompactionTest.java"
        );
        assert!(
            cov.unclassified_high
                .iter()
                .any(|f| f == "TombstonesTest.java"),
            "TombstonesTest.java must NOT be classified by RepairedDataTombstonesTest.java"
        );
    }
}
