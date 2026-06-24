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

/// Compute coverage of high-relevance files against the manifest's referenced
/// Cassandra files.
pub fn analyze(m: &Manifest, index_text: &str) -> CoverageReport {
    let high = high_relevance_files(index_text);
    let mut manifest_files: BTreeSet<String> = BTreeSet::new();
    for s in &m.scenarios {
        for f in &s.cassandra.files {
            manifest_files.insert(f.clone());
        }
    }

    let mut classified = 0usize;
    let mut unclassified = Vec::new();
    for hf in &high {
        let hit = manifest_files.iter().any(|mf| mf.contains(hf.as_str()));
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
