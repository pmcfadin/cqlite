//! Corpus IDENTITY: the recorded `sha256` + row/partition/byte shape of a
//! generated corpus (issue #3096, requirement R4).
//!
//! # The digest this corpus is NOT
//!
//! Issue #3096 quotes `0185909de6da0de839e75defe8b7113f502001017db3b5312d7ed6fd3312f0b1`
//! as the corpus digest. That digest belongs to the #3058/#3100 corpus, which was
//! **Cassandra-written and LZ4-compressed**. CQLite's write surface is
//! uncompressed-only (issue #1406), so this generator CANNOT produce those bytes
//! and this module deliberately asserts NOTHING against that value. The corpus is
//! pinned by its OWN recorded identity, written by [`CorpusIdentity::write_json`]
//! at generation time and compared on a re-run.
//!
//! # What is compared on a re-run
//!
//! `Data.db` is the load-bearing artifact for both measurement arms, so its
//! `sha256` is the primary determinism assertion. Every other emitted component
//! is recorded too (name, size, `sha256`), so a future divergence is localized
//! rather than merely detected.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

/// One emitted SSTable component.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct Component {
    /// File name, e.g. `nb-1-big-Data.db`.
    pub name: String,
    /// Size in bytes.
    pub bytes: u64,
    /// Lowercase hex `sha256` of the file's contents.
    pub sha256: String,
}

/// The recorded identity of one generated corpus.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct CorpusIdentity {
    /// Issue this rig belongs to.
    pub issue: String,
    /// The seed the corpus was generated from.
    pub seed: u64,
    /// Keyspace.table of the fixture.
    pub table: String,
    /// Rows written (counted, never assumed).
    pub rows: u64,
    /// Partitions written (reported by the writer, not by the caller's plan).
    pub partitions: u64,
    /// Rows per partition.
    pub rows_per_partition: u64,
    /// Columns per row, derived from the pinned schema.
    pub cells_per_row: usize,
    /// `Data.db` size in bytes.
    pub data_db_bytes: u64,
    /// `Data.db` lowercase hex `sha256` — the determinism assertion.
    pub data_db_sha256: String,
    /// `data_db_bytes / rows`.
    pub bytes_per_row: f64,
    /// Total bytes across every emitted component.
    pub total_component_bytes: u64,
    /// Every emitted component, keyed by file name.
    pub components: BTreeMap<String, Component>,
    /// Whether a `CompressionInfo.db` was emitted. MUST be `false` (issue #1406).
    pub compression_info_present: bool,
    /// Why this corpus is not a correctness oracle. Carried IN the artifact so a
    /// reader of the JSON alone cannot miss it (issue #3042).
    pub not_a_correctness_oracle: String,
    /// Why the #3058/#3100 digest is not asserted.
    pub differs_from_prior_corpus: String,
}

/// The standing caveat, recorded in every identity artifact.
pub const NOT_A_CORRECTNESS_ORACLE: &str =
    "PERFORMANCE FIXTURE ONLY. This corpus is CQLite-written and CQLite-read, so it is \
     INVARIANT to a uniform framing/serialization error (issue #3042) — two defects that \
     cancel are undetectable by a symmetric round trip BY CONSTRUCTION. No on-disk framing \
     or encoding correctness claim may rest on it; correctness stays anchored to the \
     Cassandra-written fixtures and the oracles in \
     openspec/changes/arrow-encode-doget/design.md.";

/// The standing statement about the prior, unreproducible corpus digest.
pub const DIFFERS_FROM_PRIOR_CORPUS: &str = "Differs from the #3058/#3100 corpus digest \
     0185909de6da0de839e75defe8b7113f502001017db3b5312d7ed6fd3312f0b1 BY CONSTRUCTION: that \
     corpus was Cassandra-written and LZ4-compressed, which CQLite's uncompressed-only write \
     surface (issue #1406) cannot reproduce. That digest is NOT asserted anywhere.";

/// Lowercase hex `sha256` of a file's contents, streamed in 1 MiB chunks so a
/// multi-GB `Data.db` is hashed under a bounded memory footprint.
pub fn sha256_file(path: &Path) -> std::io::Result<(String, u64)> {
    use std::io::Read;
    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 1 << 20];
    let mut total = 0u64;
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        total += n as u64;
        hasher.update(&buf[..n]);
    }
    Ok((format!("{:x}", hasher.finalize()), total))
}

/// Hash every regular file directly inside `dir` (the SSTable component set).
pub fn scan_components(dir: &Path) -> std::io::Result<BTreeMap<String, Component>> {
    let mut out = BTreeMap::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().to_string();
        let (sha256, bytes) = sha256_file(&path)?;
        out.insert(
            name.clone(),
            Component {
                name,
                bytes,
                sha256,
            },
        );
    }
    Ok(out)
}

impl CorpusIdentity {
    /// Serialize to pretty JSON with a trailing newline (so the committed
    /// artifact is diff-friendly).
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        Ok(serde_json::to_string_pretty(self)? + "\n")
    }

    /// Write the identity artifact, creating parent directories as needed.
    pub fn write_json(&self, path: &PathBuf) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let json = self
            .to_json()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        std::fs::write(path, json)
    }

    /// Compare against a previously recorded identity, returning every field that
    /// differs. An EMPTY vec means the corpus reproduced exactly.
    ///
    /// `Data.db` is checked first and named explicitly, because it is the artifact
    /// both measurement arms actually read.
    pub fn diff(&self, prior: &CorpusIdentity) -> Vec<String> {
        let mut out = Vec::new();
        if self.data_db_sha256 != prior.data_db_sha256 {
            out.push(format!(
                "Data.db sha256: recorded {} != regenerated {}",
                prior.data_db_sha256, self.data_db_sha256
            ));
        }
        if self.rows != prior.rows {
            out.push(format!("rows: recorded {} != {}", prior.rows, self.rows));
        }
        if self.partitions != prior.partitions {
            out.push(format!(
                "partitions: recorded {} != {}",
                prior.partitions, self.partitions
            ));
        }
        if self.data_db_bytes != prior.data_db_bytes {
            out.push(format!(
                "Data.db bytes: recorded {} != {}",
                prior.data_db_bytes, self.data_db_bytes
            ));
        }
        for (name, prior_c) in &prior.components {
            match self.components.get(name) {
                None => out.push(format!("component {name}: recorded, now MISSING")),
                Some(c) if c != prior_c => out.push(format!(
                    "component {name}: recorded {} ({} B) != {} ({} B)",
                    prior_c.sha256, prior_c.bytes, c.sha256, c.bytes
                )),
                Some(_) => {}
            }
        }
        for name in self.components.keys() {
            if !prior.components.contains_key(name) {
                out.push(format!(
                    "component {name}: NEW, not in the recorded identity"
                ));
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ident(sha: &str, rows: u64) -> CorpusIdentity {
        let mut components = BTreeMap::new();
        components.insert(
            "nb-1-big-Data.db".to_string(),
            Component {
                name: "nb-1-big-Data.db".to_string(),
                bytes: 100,
                sha256: sha.to_string(),
            },
        );
        CorpusIdentity {
            issue: "#3096".to_string(),
            seed: 1,
            table: "ws0.events".to_string(),
            rows,
            partitions: 1,
            rows_per_partition: rows,
            cells_per_row: 12,
            data_db_bytes: 100,
            data_db_sha256: sha.to_string(),
            bytes_per_row: 100.0 / rows as f64,
            total_component_bytes: 100,
            components,
            compression_info_present: false,
            not_a_correctness_oracle: NOT_A_CORRECTNESS_ORACLE.to_string(),
            differs_from_prior_corpus: DIFFERS_FROM_PRIOR_CORPUS.to_string(),
        }
    }

    #[test]
    fn identical_identities_diff_empty() {
        assert!(ident("aa", 10).diff(&ident("aa", 10)).is_empty());
    }

    /// A changed `Data.db` digest must be reported FIRST and by name — that is the
    /// determinism assertion the committed corpus rests on.
    #[test]
    fn a_changed_data_db_digest_is_reported() {
        let d = ident("bb", 10).diff(&ident("aa", 10));
        assert!(d[0].starts_with("Data.db sha256:"), "got {d:?}");
    }

    #[test]
    fn a_changed_row_count_is_reported() {
        let d = ident("aa", 11).diff(&ident("aa", 10));
        assert!(d.iter().any(|m| m.starts_with("rows:")), "got {d:?}");
    }

    /// The caveat travels IN the artifact — a reader of the JSON alone must see it.
    #[test]
    fn the_json_carries_the_performance_fixture_only_caveat() {
        let json = ident("aa", 10).to_json().expect("serialize");
        assert!(json.contains("PERFORMANCE FIXTURE ONLY"));
        assert!(json.contains("3042"));
        // And it must NOT assert the prior corpus's digest as this corpus's own.
        assert!(!json.contains("\"data_db_sha256\": \"0185909de6da"));
    }

    /// A missing or extra component must be visible, so a component-set change
    /// (e.g. a stray `CompressionInfo.db`) cannot slip through as "Data.db matched".
    #[test]
    fn component_set_changes_are_reported() {
        let prior = ident("aa", 10);
        let mut now = ident("aa", 10);
        now.components.insert(
            "nb-1-big-CompressionInfo.db".to_string(),
            Component {
                name: "nb-1-big-CompressionInfo.db".to_string(),
                bytes: 8,
                sha256: "cc".to_string(),
            },
        );
        let d = now.diff(&prior);
        assert!(
            d.iter().any(|m| m.contains("CompressionInfo.db: NEW")),
            "got {d:?}"
        );

        let d = prior.diff(&now);
        assert!(
            d.iter()
                .any(|m| m.contains("CompressionInfo.db: recorded, now MISSING")),
            "got {d:?}"
        );
    }
}
