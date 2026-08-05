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
    /// `sha256` of the emitted `ws0-events.cql` — a MEASUREMENT INPUT (#3272 R2).
    ///
    /// The DDL travels beside the corpus and is read by BOTH arms, asymmetrically: the bare
    /// scan ingests it on EVERY invocation, while the Flight ticket is generated from it ONCE.
    /// So a modification between the two makes the two arms use DIFFERENT SCHEMAS, and nothing
    /// in the recorded identity could see it — the file was outside both corpus verification and
    /// the pre-measurement pin, so the report stayed valid by its own account.
    ///
    /// Recorded as the digest of the FILE CONTENT (`DDL` + a trailing newline, which is what
    /// `generate` writes), so the recorded value is comparable against `sha256sum` on disk.
    ///
    /// # Why this is `Option`, and what the `None` state is NOT (#3272 review round 7, F1)
    ///
    /// It was a REQUIRED `String`, which broke the one command every determinism claim rests
    /// on. `--verify-against` DESERIALIZES a previously recorded identity, and the committed
    /// `docs/reports/ws0-3096-artifacts/corpus-identity.json` was recorded on 2026-08-03,
    /// BEFORE this field existed — so every documented verification command failed with
    /// `missing field schema_sha256` **before generation even began**. The retrospective check
    /// was unrunnable against the only artifact it was ever pointed at.
    ///
    /// Identities recorded before the pin genuinely exist and always will, so reading them is
    /// correct. What must NOT happen is the `None` being folded into "matches": an unobserved
    /// field treated as agreement is precisely this issue's defect class. So `None` is a
    /// DISTINGUISHABLE THIRD STATE — [`CorpusIdentity::compare`] reports it under
    /// [`IdentityComparison::unverified`], never under `divergences` and never as silence, and
    /// `main.rs` turns a non-empty `unverified` into a `PARTIAL` verdict with a NON-ZERO exit.
    /// A comparison that could not see a field does not get to say `PASS`.
    ///
    /// GENERATION always records `Some`: `generate()` hashes the file it just wrote and refuses
    /// an empty one. That is asserted affirmatively on REAL generated output by
    /// `tests/determinism_byte_compare.rs::two_independent_generations_are_byte_identical`, so
    /// `None` can only ever mean "this identity predates the pin", never "this run declined to
    /// look".
    #[serde(default)]
    pub schema_sha256: Option<String>,
}

/// The result of comparing a regenerated identity against a recorded one.
///
/// # Why this is a STRUCT and not a `Vec<String>` (#3272 review round 7, F1)
///
/// `diff` returned only DIVERGENCES, so a caller had exactly two states available to it:
/// "some fields differ" and "everything matched". There is a third, and it is the one that
/// matters here — "a field could not be compared at all, because the recorded identity does
/// not carry it". Returning that as an empty divergence list would make an UNVERIFIED field
/// read exactly like a verified one.
///
/// The two channels are returned TOGETHER, in one value, deliberately: an `unverified`
/// accessor a caller had to remember to call would be a recorded observation with no reader,
/// which is round 6's B2 lesson (a field nothing reads is not a guard). A caller that wants a
/// verdict must go through [`Self::verdict`], which cannot ignore either channel.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct IdentityComparison {
    /// Fields whose recorded and regenerated values DISAGREE. Non-empty = the corpus did not
    /// reproduce.
    pub divergences: Vec<String>,
    /// Fields the recorded identity does not carry, so nothing could be compared. Non-empty =
    /// the comparison is INCOMPLETE, which is neither a pass nor a reproduction failure.
    pub unverified: Vec<String>,
}

/// The three-way verdict of a comparison. There is deliberately no `bool`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityVerdict {
    /// Every field was compared and every field agreed.
    Reproduced,
    /// Every field that COULD be compared agreed, but at least one could not be compared.
    /// Not a pass: a check that did not run prints exactly like one that passed.
    PartialUnverified,
    /// At least one field disagreed.
    Diverged,
}

impl IdentityComparison {
    /// The verdict, derived from BOTH channels.
    ///
    /// `Diverged` wins over `PartialUnverified` because a divergence is the stronger, more
    /// actionable fact; the unverified list is still carried and still printed.
    pub fn verdict(&self) -> IdentityVerdict {
        if !self.divergences.is_empty() {
            IdentityVerdict::Diverged
        } else if !self.unverified.is_empty() {
            IdentityVerdict::PartialUnverified
        } else {
            IdentityVerdict::Reproduced
        }
    }
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

    /// Compare against a previously recorded identity.
    ///
    /// Returns BOTH channels (see [`IdentityComparison`]): the fields that DIFFER, and the
    /// fields the recorded identity does not carry so that nothing could be compared. An
    /// empty `divergences` alone does NOT mean the corpus reproduced — check
    /// [`IdentityComparison::verdict`].
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
    pub fn compare(&self, prior: &CorpusIdentity) -> IdentityComparison {
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
            schema_sha256,
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
            schema_sha256: p_schema_sha256,
        } = prior;

        let mut out = Vec::new();
        let mut unverified = Vec::new();
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
        // THE SCHEMA (#3272 R2). A measurement input read by BOTH arms — asymmetrically, so a
        // change between the two makes them use DIFFERENT SCHEMAS — and it was outside every
        // recorded identity, so nothing could see it.
        //
        // THREE states, not two (#3272 review round 7, F1). A prior recorded BEFORE the pin
        // existed carries no digest, and an absent digest is neither agreement nor divergence:
        // it is the comparison NOT HAVING HAPPENED, which goes to `unverified` and makes the
        // verdict `PartialUnverified`. Folding it into "matches" is the fail-open shape this
        // whole issue exists to remove.
        match (schema_sha256, p_schema_sha256) {
            (Some(now), Some(prior)) if now != prior => {
                out.push(format!("ws0-events.cql sha256: recorded {prior} != {now}"))
            }
            (Some(_), Some(_)) => {}
            (now, None) => unverified.push(format!(
                "ws0-events.cql sha256: the recorded identity carries NO `schema_sha256`, so the \
                 SCHEMA both measurement arms read was NOT compared. This identity predates the \
                 #3272 R2 schema pin (the committed \
                 docs/reports/ws0-3096-artifacts/corpus-identity.json was recorded 2026-08-03, \
                 before the field existed). The regenerated corpus's schema digest is {}. To \
                 VERIFY the schema, re-record the prior identity with a generator that emits \
                 the field.",
                now.as_deref().unwrap_or("also absent")
            )),
            // A prior that HAS the digest compared against a regenerated identity that does
            // not is a different fault: `generate()` always records it, so this can only be a
            // hand-edited or truncated identity. Reported as a DIVERGENCE, because something
            // that should be present is missing.
            (None, Some(prior)) => out.push(format!(
                "ws0-events.cql sha256: recorded {prior} but the regenerated identity carries \
                 NONE — `generate()` always records this digest, so this identity was \
                 hand-edited rather than generated"
            )),
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
        IdentityComparison {
            divergences: out,
            unverified,
        }
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
            schema_sha256: Some("cc".to_string()),
        }
    }

    /// Every field name a [`CorpusIdentity`] actually carries, **DERIVED FROM THE STRUCT**
    /// through serde rather than counted by hand (#3272 review round 7, F5).
    ///
    /// # Why the hand-written count had to go
    ///
    /// The backstop below used to end in `assert_eq!(mutations.len(), 15)`, whose stated
    /// purpose (in the doc comment two paragraphs down) was to FORCE a future field to get a
    /// divergence case. R2 then added a 16th field, `schema_sha256`, and the count stayed at
    /// `15` — so the assert PASSED while the new field had no case at all. The mechanism meant
    /// to catch exactly that could not, because a hardcoded number is not a measurement of the
    /// struct: it is a second copy of a fact, free to drift from the first. Bumping it to `16`
    /// would reinstate the same shape for the 17th field.
    ///
    /// serde's derived `Serialize` emits one key per field, so the serialized key set IS the
    /// field set — read from the type at runtime, with no reflection and no macro. A 17th field
    /// therefore appears here the moment it is added to the struct, and the coverage assertion
    /// below FAILS naming it.
    ///
    /// This is a genuine derivation and not a cleverer enumeration: nothing in this function
    /// mentions any field name.
    fn corpus_identity_field_names() -> Vec<String> {
        let value = serde_json::to_value(ident("aa", 10)).expect("an identity serializes");
        let obj = value
            .as_object()
            .expect("CorpusIdentity serializes to a JSON object");
        // A struct with no fields would make the coverage check below vacuous, so the subject
        // is asserted non-empty rather than trusted — the same rule the rest of this issue
        // applies to every other subject.
        assert!(
            !obj.is_empty(),
            "the serialized identity carries NO keys, so the coverage check below would have \
             an empty subject and pass having compared nothing"
        );
        obj.keys().cloned().collect()
    }

    #[test]
    fn identical_identities_diff_empty() {
        let cmp = ident("aa", 10).compare(&ident("aa", 10));
        assert!(cmp.divergences.is_empty(), "got {cmp:?}");
        // ...and nothing UNVERIFIED either: two identities that both carry every field are
        // fully compared, so the verdict must be the strong one.
        assert_eq!(cmp.verdict(), IdentityVerdict::Reproduced, "got {cmp:?}");
    }

    /// A changed `Data.db` digest must be reported FIRST and by name — that is the
    /// determinism assertion the committed corpus rests on.
    #[test]
    fn a_changed_data_db_digest_is_reported() {
        let d = ident("bb", 10).compare(&ident("aa", 10)).divergences;
        assert!(d[0].starts_with("Data.db sha256:"), "got {d:?}");
    }

    #[test]
    fn a_changed_row_count_is_reported() {
        let d = ident("aa", 11).compare(&ident("aa", 10)).divergences;
        assert!(d.iter().any(|m| m.starts_with("rows:")), "got {d:?}");
    }

    /// A CHANGED schema digest is a DIVERGENCE (#3272 R2), and it is named.
    #[test]
    fn a_changed_schema_digest_is_reported() {
        let mut now = ident("aa", 10);
        now.schema_sha256 = Some("dd".to_string());
        let d = now.compare(&ident("aa", 10)).divergences;
        assert!(
            d.iter().any(|m| m.starts_with("ws0-events.cql sha256:")),
            "got {d:?}"
        );
    }

    /// THE THIRD STATE (#3272 review round 7, F1): a prior with NO schema digest is
    /// `unverified`, never `divergences`, and NEVER `Reproduced`.
    ///
    /// This is the assertion whose absence would let the fail-open read ship: an `Option` field
    /// compared with `!=` would have made `None == None` read as agreement, and a `None` prior
    /// against a `Some` regeneration read as a divergence about a schema that never changed.
    #[test]
    fn a_prior_without_a_schema_digest_is_unverified_not_reproduced() {
        let mut prior = ident("aa", 10);
        prior.schema_sha256 = None;
        let cmp = ident("aa", 10).compare(&prior);
        assert!(
            cmp.divergences.is_empty(),
            "an absent recorded digest is not a DIVERGENCE — nothing disagreed; got {cmp:?}"
        );
        assert_eq!(
            cmp.unverified.len(),
            1,
            "the absent schema digest must be reported as UNVERIFIED; got {cmp:?}"
        );
        assert!(
            cmp.unverified[0].contains("NO `schema_sha256`"),
            "the unverified entry must name what could not be compared; got {cmp:?}"
        );
        assert_eq!(
            cmp.verdict(),
            IdentityVerdict::PartialUnverified,
            "a comparison that could not see a field must NOT read as Reproduced; got {cmp:?}"
        );
        // NON-VACUITY, stated as the failing alternative: had the field been compared with a
        // plain `!=` on the `Option` (the smaller edit), `None` vs `None` would have compared
        // EQUAL and the verdict would have been `Reproduced` — a schema that was never checked
        // reported as reproduced. Driven here rather than argued.
        let mut both_absent_prior = ident("aa", 10);
        both_absent_prior.schema_sha256 = None;
        let mut both_absent_now = ident("aa", 10);
        both_absent_now.schema_sha256 = None;
        assert_eq!(
            both_absent_now.schema_sha256, both_absent_prior.schema_sha256,
            "the pre-fix `!=` comparison really did see these two as EQUAL — which is why \
             `None` had to become a third state rather than a compared value"
        );
        let cmp = both_absent_now.compare(&both_absent_prior);
        assert_eq!(
            cmp.verdict(),
            IdentityVerdict::PartialUnverified,
            "two identities that BOTH lack the digest are still UNVERIFIED, not reproduced"
        );
    }

    /// A regenerated identity missing the digest a prior HAS is a divergence, not an
    /// unverified field: `generate()` always records it, so its absence means a hand-edited
    /// identity — a different fault with a different remedy.
    #[test]
    fn a_regenerated_identity_missing_the_schema_digest_is_a_divergence() {
        let mut now = ident("aa", 10);
        now.schema_sha256 = None;
        let cmp = now.compare(&ident("aa", 10));
        assert!(
            cmp.divergences
                .iter()
                .any(|m| m.contains("hand-edited rather than generated")),
            "got {cmp:?}"
        );
        assert_eq!(cmp.verdict(), IdentityVerdict::Diverged, "got {cmp:?}");
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
    /// `(current divergences, pre-review diff)` for it.
    fn diverge(mutate: impl FnOnce(&mut CorpusIdentity)) -> (Vec<String>, Vec<String>) {
        let prior = ident("aa", 10);
        let mut now = ident("aa", 10);
        mutate(&mut now);
        (
            now.compare(&prior).divergences,
            diff_pre_review(&now, &prior),
        )
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

    /// One perturbation of an identity, applied to a single field.
    type FieldMutation = fn(&mut CorpusIdentity);

    /// One perturbation per field of [`CorpusIdentity`], KEYED BY THE FIELD'S SERDE NAME.
    ///
    /// Separate from the assertions so BOTH the per-field property and the
    /// COVERAGE-OF-THE-STRUCT property can be driven from the same single source, rather than
    /// one list plus a hand-written count of it (#3272 review round 7, F5).
    fn single_field_mutations() -> Vec<(&'static str, FieldMutation)> {
        vec![
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
            // The R2 field. Its case was MISSING for a whole review round while the
            // `mutations.len() == 15` assert passed (#3272 F5), which is why the coverage
            // check below is now derived from the struct instead of counted here.
            ("schema_sha256", |i| {
                i.schema_sha256 = Some("dd".to_string())
            }),
        ]
    }

    /// EVERY field of [`CorpusIdentity`] has a divergence case — DERIVED FROM THE STRUCT.
    ///
    /// # The mechanism, and why the count it replaces was the defect
    ///
    /// This used to be `assert_eq!(mutations.len(), 15)` inside the per-field test, whose
    /// stated purpose was to force a NEW field to acquire a case. It could not: R2 added
    /// `schema_sha256` and left the number at 15, so the assert passed with the new field
    /// uncovered — the guard satisfied without covering its subject. Bumping the literal would
    /// reinstate exactly that for the 17th field.
    ///
    /// So the subject is READ FROM THE TYPE ([`corpus_identity_field_names`], via serde's
    /// derived `Serialize`) and the mutation set is compared against it in BOTH directions:
    ///
    ///  * a field of the struct with NO mutation case FAILS, naming the field. This is the
    ///    property the count was meant to have.
    ///  * a mutation case naming a field the struct does not have FAILS too, so a rename
    ///    leaves a case pointing at nothing rather than silently testing a field that is gone.
    ///
    /// Nothing here mentions a field name or a count, so a 17th field cannot compile-and-pass.
    #[test]
    fn every_identity_field_has_a_divergence_case() {
        let fields = corpus_identity_field_names();
        let cases: Vec<&str> = single_field_mutations().iter().map(|(k, _)| *k).collect();

        let uncovered: Vec<&String> = fields
            .iter()
            .filter(|f| !cases.contains(&f.as_str()))
            .collect();
        assert!(
            uncovered.is_empty(),
            "CorpusIdentity carries field(s) {uncovered:?} with NO divergence case in \
             `single_field_mutations`. This subject is DERIVED from the struct via serde, \
             precisely because the `mutations.len() == 15` assert it replaced passed while R2's \
             `schema_sha256` had no case at all (#3272 F5). Add the case; do not relax this."
        );

        let stale: Vec<&&str> = cases
            .iter()
            .filter(|c| !fields.contains(&c.to_string()))
            .collect();
        assert!(
            stale.is_empty(),
            "`single_field_mutations` names field(s) {stale:?} that CorpusIdentity does not \
             carry — a renamed or removed field leaves a case testing nothing"
        );
    }

    /// The backstop property, stated directly: for EVERY field, a divergence in it alone must
    /// produce a non-empty divergence list. Coverage of the field set is asserted separately by
    /// [`every_identity_field_has_a_divergence_case`], which derives it from the struct.
    #[test]
    fn no_single_field_divergence_reads_as_reproduced_exactly() {
        for (field, mutate) in single_field_mutations() {
            let (now, _) = diverge(mutate);
            assert!(
                !now.is_empty(),
                "a divergence in `{field}` alone read as 'reproduced exactly'"
            );
        }
    }

    /// NON-VACUITY for the coverage check above: it must FAIL on a field with no case.
    ///
    /// Driven by SUBTRACTING a case from the real set and re-running the same comparison the
    /// assertion performs — so "the coverage check has teeth" is observed rather than asserted
    /// of the code. Without this, a `corpus_identity_field_names` that returned an empty vec
    /// (or a `contains` that always answered true) would leave the check green forever, which
    /// is the shape the count-assert died of.
    #[test]
    fn the_field_coverage_check_fires_on_an_uncovered_field() {
        let fields = corpus_identity_field_names();
        // Drop the R2 field's case — the exact state round 7 found in the tree.
        let cases: Vec<&str> = single_field_mutations()
            .iter()
            .map(|(k, _)| *k)
            .filter(|k| *k != "schema_sha256")
            .collect();
        let uncovered: Vec<&String> = fields
            .iter()
            .filter(|f| !cases.contains(&f.as_str()))
            .collect();
        assert_eq!(
            uncovered.len(),
            1,
            "removing ONE case must leave exactly one field uncovered; got {uncovered:?}"
        );
        assert_eq!(
            uncovered[0], "schema_sha256",
            "the uncovered field must be NAMED, so the failure is actionable"
        );
        // ...and the positive control: with the case present, nothing is uncovered. A check
        // that reported an uncovered field unconditionally would satisfy the half above.
        let all: Vec<&str> = single_field_mutations().iter().map(|(k, _)| *k).collect();
        assert!(
            fields.iter().all(|f| all.contains(&f.as_str())),
            "with every case present the check must find NOTHING uncovered"
        );
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
        let d = now.compare(&prior).divergences;
        assert!(
            d.iter().any(|m| m.contains("CompressionInfo.db: NEW")),
            "got {d:?}"
        );

        let d = prior.compare(&now).divergences;
        assert!(
            d.iter()
                .any(|m| m.contains("CompressionInfo.db: recorded, now MISSING")),
            "got {d:?}"
        );
    }
}
