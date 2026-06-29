//! Manifest-driven public-claim scanning (issue #1023).
//!
//! Acceptance criteria 4 & 5: every public parity claim in release-facing docs
//! must reference manifest evidence or be rejected, and unqualified absolute
//! phrases (e.g. "same tests as Cassandra", "full compaction byte parity",
//! "zero-diff sstabledump across every dataset") fail lint unless explicitly
//! scoped.
//!
//! The authoritative source of which phrases are public claims is the manifest
//! `claims:` section (no hard-coded phrase list here): `claim.blocked.*` entries
//! supply the literal over-claim phrases to scan for, and `claim.safe.*` entries
//! supply the manifest-backed wording that is allowed to appear verbatim.
//!
//! A blocked phrase occurrence is allowed only when its line is **explicitly
//! scoped** — i.e. it is framed as a counter-example ("unsafe", "do not claim",
//! "reject", quoted as a negative) or it is the manifest-anchored safe wording.
//! A bare assertion of a blocked phrase fails lint.

use crate::lint::{Finding, Level};
use crate::model::Manifest;

/// One release-facing file to scan: a repo-relative display path and its text.
pub struct ScanInput<'a> {
    pub path: &'a str,
    pub text: &'a str,
}

/// Lowercased markers that, when present on the same line as a blocked phrase,
/// indicate the phrase is being explicitly scoped/negated rather than asserted.
const SCOPE_MARKERS: &[&str] = &[
    "unsafe",
    "do not",
    "don't",
    "not claim",
    "never claim",
    "must not",
    "reject",
    "rejected",
    "out of scope",
    "out-of-scope",
    "overclaim",
    "over-claim",
    "avoid",
    "no unqualified",
    "instead of",
    "rather than",
    "counter-example",
    "anti-pattern",
];

/// Normalize a string for phrase matching: lowercase and collapse runs of
/// whitespace (so a phrase split across a soft-wrap still matches).
fn normalize(s: &str) -> String {
    s.to_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// True if the line frames the phrase as a scoped/negated counter-example.
fn line_is_scoped(line_lower: &str) -> bool {
    SCOPE_MARKERS.iter().any(|m| line_lower.contains(m))
}

/// Scan the given release-facing files for unqualified public-claim phrases.
///
/// For each `claim.blocked.*` entry, every release file is searched for the
/// claim phrase. An occurrence is a lint error unless the line it appears on is
/// explicitly scoped (see [`SCOPE_MARKERS`]) or is the verbatim wording of a
/// `claim.safe.*` entry. Findings name the file, line, claim id, and the safe
/// alternative to use instead.
pub fn scan_docs(m: &Manifest, files: &[ScanInput<'_>]) -> Vec<Finding> {
    let mut out = Vec::new();

    let blocked: Vec<&crate::model::Claim> =
        m.claims.iter().filter(|c| c.kind == "blocked").collect();
    let safe_phrases: Vec<String> = m
        .claims
        .iter()
        .filter(|c| c.kind == "safe")
        .map(|c| normalize(&c.phrase))
        .collect();

    for f in files {
        for (lineno, raw) in f.text.lines().enumerate() {
            let line_norm = normalize(raw);
            if line_norm.is_empty() {
                continue;
            }
            // A line that is (or contains) a verbatim safe wording is allowed
            // even if a blocked substring overlaps it.
            let is_safe_wording = safe_phrases.iter().any(|p| line_norm.contains(p.as_str()));
            for c in &blocked {
                let phrase = normalize(&c.phrase);
                if phrase.is_empty() || !line_norm.contains(&phrase) {
                    continue;
                }
                if is_safe_wording || line_is_scoped(&line_norm) {
                    continue;
                }
                let alt = c
                    .safe_alternative
                    .as_deref()
                    .map(|a| format!(" Use the manifest-backed wording `{a}` instead."))
                    .unwrap_or_default();
                out.push(Finding {
                    level: Level::Error,
                    id: c.id.clone(),
                    field: format!("{}:{}", f.path, lineno + 1),
                    message: format!(
                        "unqualified public parity claim \"{}\" — must be explicitly scoped or dropped.{alt}",
                        c.phrase.trim()
                    ),
                });
            }
        }
    }

    out
}

/// The curated, conservative set of release-facing files the claim scan reads,
/// expressed as repo-relative paths. These are the public/marketing-adjacent
/// surfaces where an over-claim would actually ship; the giant generated indices
/// under `docs/` are intentionally excluded.
pub const RELEASE_FILES: &[&str] = &[
    "README.md",
    "CHANGELOG.md",
    "docs/development/parity-ci-tiers.md",
    "docs/development/parity-release-checklist.md",
    "docs/development/cassandra-parity-manifest.md",
];
