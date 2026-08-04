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
    ///
    /// # Why this is exhaustive by construction (issue #3096 review, finding 3)
    ///
    /// This function used to compare **4 of the 15 recorded fields**
    /// (`data_db_sha256`, `rows`, `partitions`, `data_db_bytes`, plus the component
    /// map) and returned an empty vec for a divergence in any of the others — so a
    /// corpus regenerated with a DIFFERENT `seed`, a different `table`, a different
    /// `rows_per_partition`/`cells_per_row`, a stray `compression_info_present`, or
    /// with the recorded caveats edited out was reported as **"reproduced
    /// exactly."** That is a FALSE VERIFICATION on the pin every future comparison
    /// rests on, and it would have propagated: this generator is the template
    /// #3232/#3234 are pointed at.
    ///
    /// The fix is structural, not a longer list: both operands are **destructured**
    /// with full field patterns, so adding a field to [`CorpusIdentity`] without
    /// extending this comparison is a COMPILE ERROR ("pattern does not mention
    /// field") rather than a silently unchecked field.
    pub fn diff(&self, prior: &CorpusIdentity) -> Vec<String> {
        // Exhaustiveness enforcement — see the doc comment above. Do NOT replace
        // these patterns with field access.
        let Self {
            issue,
            seed,
            table,
            rows,
            partitions,
            rows_per_partition,
            cells_per_row,
            data_db_bytes,
            data_db_sha256,
            bytes_per_row,
            total_component_bytes,
            components,
            compression_info_present,
            not_a_correctness_oracle,
            differs_from_prior_corpus,
        } = self;
        let Self {
            issue: p_issue,
            seed: p_seed,
            table: p_table,
            rows: p_rows,
            partitions: p_partitions,
            rows_per_partition: p_rows_per_partition,
            cells_per_row: p_cells_per_row,
            data_db_bytes: p_data_db_bytes,
            data_db_sha256: p_data_db_sha256,
            bytes_per_row: p_bytes_per_row,
            total_component_bytes: p_total_component_bytes,
            components: p_components,
            compression_info_present: p_compression_info_present,
            not_a_correctness_oracle: p_not_a_correctness_oracle,
            differs_from_prior_corpus: p_differs_from_prior_corpus,
        } = prior;

        let mut out = Vec::new();
        // `Data.db` first and by name: it is the artifact both measurement arms
        // read, so it is the primary determinism assertion.
        if data_db_sha256 != p_data_db_sha256 {
            out.push(format!(
                "Data.db sha256: recorded {p_data_db_sha256} != regenerated {data_db_sha256}"
            ));
        }
        if data_db_bytes != p_data_db_bytes {
            out.push(format!(
                "Data.db bytes: recorded {p_data_db_bytes} != {data_db_bytes}"
            ));
        }
        // Corpus SHAPE. Any of these differing means the two identities describe
        // different corpora, whatever the digests say.
        if rows != p_rows {
            out.push(format!("rows: recorded {p_rows} != {rows}"));
        }
        if partitions != p_partitions {
            out.push(format!(
                "partitions: recorded {p_partitions} != {partitions}"
            ));
        }
        if rows_per_partition != p_rows_per_partition {
            out.push(format!(
                "rows_per_partition: recorded {p_rows_per_partition} != {rows_per_partition}"
            ));
        }
        if cells_per_row != p_cells_per_row {
            out.push(format!(
                "cells_per_row: recorded {p_cells_per_row} != {cells_per_row}"
            ));
        }
        // Compared on the BIT PATTERN, not with `==`: an exact reproduction of a
        // recorded value is the property, and bit equality makes a recorded NaN
        // compare equal to itself instead of reporting a phantom difference on
        // every re-run.
        if bytes_per_row.to_bits() != p_bytes_per_row.to_bits() {
            out.push(format!(
                "bytes_per_row: recorded {p_bytes_per_row} != {bytes_per_row}"
            ));
        }
        if total_component_bytes != p_total_component_bytes {
            out.push(format!(
                "total_component_bytes: recorded {p_total_component_bytes} != {total_component_bytes}"
            ));
        }
        // PROVENANCE. A different seed or table reproduces different bytes by
        // construction, so a match on these is part of what "reproduced" means.
        if seed != p_seed {
            out.push(format!("seed: recorded {p_seed} != {seed}"));
        }
        if table != p_table {
            out.push(format!("table: recorded {p_table} != {table}"));
        }
        if issue != p_issue {
            out.push(format!("issue: recorded {p_issue} != {issue}"));
        }
        // Issue #1406: a `CompressionInfo.db` MUST NOT appear. The component loop
        // below would catch the file, but the recorded FLAG can disagree with the
        // component set (an internally inconsistent identity), and that
        // disagreement is exactly what must not read as "reproduced exactly".
        if compression_info_present != p_compression_info_present {
            out.push(format!(
                "compression_info_present: recorded {p_compression_info_present} != \
                 {compression_info_present}"
            ));
        }
        // The CAVEATS travel IN the artifact (#3042). An identity whose caveat text
        // was edited or dropped is not the recorded identity, and silently
        // accepting it is how a performance fixture gets re-labelled as an oracle.
        if not_a_correctness_oracle != p_not_a_correctness_oracle {
            out.push(format!(
                "not_a_correctness_oracle: recorded caveat text differs \
                 (recorded {} chars, now {} chars) — the caveat travels IN the artifact (#3042)",
                p_not_a_correctness_oracle.len(),
                not_a_correctness_oracle.len()
            ));
        }
        if differs_from_prior_corpus != p_differs_from_prior_corpus {
            out.push(format!(
                "differs_from_prior_corpus: recorded caveat text differs \
                 (recorded {} chars, now {} chars)",
                p_differs_from_prior_corpus.len(),
                differs_from_prior_corpus.len()
            ));
        }
        for (name, prior_c) in p_components {
            match components.get(name) {
                None => out.push(format!("component {name}: recorded, now MISSING")),
                Some(c) if c != prior_c => out.push(format!(
                    "component {name}: recorded {} ({} B) != {} ({} B)",
                    prior_c.sha256, prior_c.bytes, c.sha256, c.bytes
                )),
                Some(_) => {}
            }
        }
        for name in components.keys() {
            if !p_components.contains_key(name) {
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

    /// The comparison `diff` performed BEFORE the issue-#3096 review (finding 3),
    /// kept as a **non-vacuity oracle** for the per-field tests below.
    ///
    /// Each per-field test asserts BOTH halves: the current `diff` reports the
    /// divergence, AND this pre-fix comparison did NOT — i.e. the same input really
    /// did read as "reproduced exactly" before the fix. Without the second half a
    /// passing test proves nothing about whether the guard was ever broken.
    ///
    /// This is a frozen historical replica. It must never be called by production
    /// code and must never be "kept in sync" with `diff`.
    fn diff_pre_review(now: &CorpusIdentity, prior: &CorpusIdentity) -> Vec<String> {
        let mut out = Vec::new();
        if now.data_db_sha256 != prior.data_db_sha256 {
            out.push("Data.db sha256".to_string());
        }
        if now.rows != prior.rows {
            out.push("rows".to_string());
        }
        if now.partitions != prior.partitions {
            out.push("partitions".to_string());
        }
        if now.data_db_bytes != prior.data_db_bytes {
            out.push("Data.db bytes".to_string());
        }
        for (name, prior_c) in &prior.components {
            match now.components.get(name) {
                None => out.push(format!("component {name} MISSING")),
                Some(c) if c != prior_c => out.push(format!("component {name} changed")),
                Some(_) => {}
            }
        }
        for name in now.components.keys() {
            if !prior.components.contains_key(name) {
                out.push(format!("component {name} NEW"));
            }
        }
        out
    }

    /// Mutate one field of an otherwise byte-identical identity and return
    /// `(current diff, pre-review diff)` for it.
    fn diverge(mutate: impl FnOnce(&mut CorpusIdentity)) -> (Vec<String>, Vec<String>) {
        let prior = ident("aa", 10);
        let mut now = ident("aa", 10);
        mutate(&mut now);
        (now.diff(&prior), diff_pre_review(&now, &prior))
    }

    /// Every field the pre-review `diff` ignored is now reported — and each case
    /// proves its own non-vacuity: the pre-review comparison returned EMPTY, i.e.
    /// "reproduced exactly", for the very same divergence.
    ///
    /// One test per field rather than a loop, so a failure names the field.
    macro_rules! previously_ignored_field {
        ($test:ident, $prefix:literal, $mutate:expr) => {
            #[test]
            fn $test() {
                let (now, pre) = diverge($mutate);
                assert!(
                    now.iter().any(|m| m.starts_with($prefix)),
                    "diff must report a {} divergence; got {now:?}",
                    $prefix
                );
                assert!(
                    pre.is_empty(),
                    "NON-VACUITY: the pre-review diff must have reported this as \
                     'reproduced exactly'; got {pre:?}"
                );
            }
        };
    }

    previously_ignored_field!(
        a_changed_seed_is_reported,
        "seed:",
        |i: &mut CorpusIdentity| i.seed = 2
    );
    previously_ignored_field!(
        a_changed_table_is_reported,
        "table:",
        |i: &mut CorpusIdentity| { i.table = "ws0.other".to_string() }
    );
    previously_ignored_field!(
        a_changed_issue_is_reported,
        "issue:",
        |i: &mut CorpusIdentity| { i.issue = "#0000".to_string() }
    );
    previously_ignored_field!(
        a_changed_rows_per_partition_is_reported,
        "rows_per_partition:",
        |i: &mut CorpusIdentity| i.rows_per_partition = 7
    );
    previously_ignored_field!(
        a_changed_cells_per_row_is_reported,
        "cells_per_row:",
        |i: &mut CorpusIdentity| i.cells_per_row = 11
    );
    previously_ignored_field!(
        a_changed_bytes_per_row_is_reported,
        "bytes_per_row:",
        |i: &mut CorpusIdentity| i.bytes_per_row = 99.0
    );
    previously_ignored_field!(
        a_changed_total_component_bytes_is_reported,
        "total_component_bytes:",
        |i: &mut CorpusIdentity| i.total_component_bytes = 101
    );
    // Issue #1406: the FLAG can disagree with the component set. That is an
    // internally inconsistent identity, and it used to read as reproduced exactly.
    previously_ignored_field!(
        a_flipped_compression_info_flag_is_reported,
        "compression_info_present:",
        |i: &mut CorpusIdentity| i.compression_info_present = true
    );
    previously_ignored_field!(
        an_edited_correctness_caveat_is_reported,
        "not_a_correctness_oracle:",
        |i: &mut CorpusIdentity| i.not_a_correctness_oracle = "it is fine actually".to_string()
    );
    previously_ignored_field!(
        an_edited_prior_corpus_caveat_is_reported,
        "differs_from_prior_corpus:",
        |i: &mut CorpusIdentity| i.differs_from_prior_corpus = String::new()
    );

    /// The backstop property, stated directly: for EVERY field, a divergence in it
    /// alone must produce a non-empty diff. Enumerated here as a set so a future
    /// field added to `CorpusIdentity` (which the destructure in `diff` forces the
    /// author to handle) also gets its "must be reported" assertion here.
    #[test]
    fn no_single_field_divergence_reads_as_reproduced_exactly() {
        type Mut = fn(&mut CorpusIdentity);
        let mutations: Vec<(&str, Mut)> = vec![
            ("issue", |i| i.issue = "#0000".to_string()),
            ("seed", |i| i.seed = 2),
            ("table", |i| i.table = "ws0.other".to_string()),
            ("rows", |i| i.rows = 11),
            ("partitions", |i| i.partitions = 2),
            ("rows_per_partition", |i| i.rows_per_partition = 7),
            ("cells_per_row", |i| i.cells_per_row = 11),
            ("data_db_bytes", |i| i.data_db_bytes = 101),
            ("data_db_sha256", |i| i.data_db_sha256 = "bb".to_string()),
            ("bytes_per_row", |i| i.bytes_per_row = 99.0),
            ("total_component_bytes", |i| i.total_component_bytes = 101),
            ("components", |i| {
                i.components.clear();
            }),
            ("compression_info_present", |i| {
                i.compression_info_present = true
            }),
            ("not_a_correctness_oracle", |i| {
                i.not_a_correctness_oracle = "nope".to_string()
            }),
            ("differs_from_prior_corpus", |i| {
                i.differs_from_prior_corpus = "nope".to_string()
            }),
        ];
        // Every field of CorpusIdentity is covered — the count is asserted so a new
        // field cannot be added with no case here.
        assert_eq!(
            mutations.len(),
            15,
            "CorpusIdentity has 15 fields; add the new field's divergence case"
        );
        for (field, mutate) in mutations {
            let (now, _) = diverge(mutate);
            assert!(
                !now.is_empty(),
                "a divergence in `{field}` alone read as 'reproduced exactly'"
            );
        }
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
