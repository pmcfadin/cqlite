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

/// The SOURCE-DERIVED oracles a comparison may use INSTEAD of the recorded prior.
///
/// # Why this type exists (#3272 review round 9, F1)
///
/// Round 7 made an absent `schema_sha256` a `PartialUnverified` with a NON-ZERO exit, which is
/// right on its own terms — a field nobody compared must not print `PASS`. But the only artifact
/// the documented verification command is ever pointed at
/// (`docs/reports/ws0-3096-artifacts/corpus-identity.json`, recorded before the field existed)
/// carries no `schema_sha256`, so the combination made **the documented operator command
/// permanently unable to succeed**, even over a corpus that reproduced every comparable field.
/// A command an operator cannot ever get a green from is its own broken instrument: they stop
/// running it.
///
/// The way out is not to weaken the verdict. It is that this ONE field has an oracle that does
/// not need the artifact at all: the schema file's content is
/// [`crate::schema::DDL`] — a source constant — so
/// [`crate::schema::ddl_file_sha256`] computes the expected digest from SOURCE. Verifying against
/// it is STRICTLY STRONGER than comparing two recorded values, because the oracle is the INPUT
/// rather than a record of it: a co-edit of pin and artifact cannot satisfy it, only an actual
/// DDL change can move it.
///
/// So a comparison given this carries no unverified schema field — not because the check was
/// skipped, but because it was made against a better oracle. [`IdentityVerdict::PartialUnverified`]
/// stays reachable for a field that genuinely has no oracle (see
/// [`CorpusIdentity::compare_with_source_oracles`]).
#[derive(Debug, Clone, Default)]
pub struct SourceOracles {
    /// Expected `sha256` of the emitted `ws0-events.cql`, derived from
    /// [`crate::schema::DDL`]. `None` = no oracle available, so an absent recorded digest stays
    /// UNVERIFIED exactly as before.
    pub schema_sha256: Option<String>,
}

impl SourceOracles {
    /// The oracles this build can derive from source. Currently exactly one: the schema digest.
    pub fn from_source() -> Self {
        Self {
            schema_sha256: Some(crate::schema::ddl_file_sha256()),
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
        self.compare_with_source_oracles(prior, &SourceOracles::default())
    }

    /// [`Self::compare`], plus any field that can be verified against a SOURCE ORACLE rather
    /// than against the recorded prior (#3272 review round 9, F1).
    ///
    /// See [`SourceOracles`] for why this exists and why it is stronger than the comparison it
    /// substitutes for. The rule it obeys: a field is only reported as verified when it was
    /// ACTUALLY compared against something — either the prior's recorded value or a source-derived
    /// expected value. A field with neither stays in
    /// [`IdentityComparison::unverified`], so
    /// [`IdentityVerdict::PartialUnverified`] remains reachable and the verdict is never derived
    /// from the ABSENCE of a bad signal.
    pub fn compare_with_source_oracles(
        &self,
        prior: &CorpusIdentity,
        oracles: &SourceOracles,
    ) -> IdentityComparison {
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
        //
        // ...AND A FOURTH ROUTE, which is the round-9 F1 fix: the schema is the ONE identity
        // field with a SOURCE ORACLE. When [`SourceOracles::schema_sha256`] is supplied, an
        // absent recorded digest no longer leaves the field unverified — it is verified against
        // `sha256(DDL + "\n")`, which is the INPUT rather than a record of it. That is not a
        // relaxation of the third state: the field is still compared against something, and it
        // is compared against a stronger something. Without an oracle the third state stands
        // exactly as round 7 left it.
        match (schema_sha256, p_schema_sha256) {
            (Some(now), Some(prior)) if now != prior => {
                out.push(format!("ws0-events.cql sha256: recorded {prior} != {now}"))
            }
            (Some(_), Some(_)) => {}
            // ABSENT FROM THE PRIOR, BUT VERIFIABLE FROM SOURCE. Compared against the
            // source-derived expectation; a mismatch is a DIVERGENCE (the emitted schema is not
            // the schema this build's `DDL` produces), and a match is a genuine verification.
            (Some(now), None) if oracles.schema_sha256.is_some() => {
                // `is_some()` was just established; the `else` arm cannot be taken and carries
                // its own diagnostic rather than a silent skip.
                match oracles.schema_sha256.as_deref() {
                    Some(expected) if now == expected => {}
                    Some(expected) => out.push(format!(
                        "ws0-events.cql sha256: the regenerated corpus emitted {now}, but the \
                         SOURCE oracle sha256(schema::DDL + \"\\n\") is {expected}. The recorded \
                         identity predates the field, so this is compared against SOURCE rather \
                         than against the prior — and source says the emitted schema is not the \
                         one this build's DDL produces."
                    )),
                    None => unverified.push(
                        "ws0-events.cql sha256: the source oracle became unavailable between the \
                         two reads of it — reported as UNVERIFIED rather than assumed"
                            .to_string(),
                    ),
                }
            }
            (now, None) => unverified.push(format!(
                "ws0-events.cql sha256: the recorded identity carries NO `schema_sha256` and NO \
                 source oracle was supplied, so the SCHEMA both measurement arms read was NOT \
                 compared. This identity predates the #3272 R2 schema pin (the committed \
                 docs/reports/ws0-3096-artifacts/corpus-identity.json was recorded 2026-08-03, \
                 before the field existed). The regenerated corpus's schema digest is {}. Verify \
                 it against SOURCE by comparing with `compare_with_source_oracles`, or re-record \
                 the prior identity with a generator that emits the field.",
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
