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
/// whitespace. Used to canonicalize a manifest phrase before matching it against
/// the normalized whole-file view (see [`NormalizedFile`]).
fn normalize(s: &str) -> String {
    s.to_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// A whole-file view normalized to lowercase with all whitespace runs (including
/// newlines) collapsed to single spaces, so a phrase split across a soft-wrap
/// still matches. `offsets[i]` maps byte index `i` of `text` back to the 1-based
/// source line number it originated from, so a match reports the line where the
/// phrase starts.
struct NormalizedFile {
    /// Lowercased, whitespace-collapsed text of the whole file.
    text: String,
    /// Per-byte source-line map: `offsets[i]` is the 1-based source line that the
    /// byte at `text[i]` came from. Length always equals `text.len()`.
    offsets: Vec<usize>,
}

impl NormalizedFile {
    /// Build the normalized view of `raw`, recording the source line each emitted
    /// byte came from. A run of whitespace collapses to one space attributed to
    /// the line where the run *started* (so a phrase wrapped across a soft-wrap
    /// reports its starting line).
    fn new(raw: &str) -> Self {
        let lower = raw.to_lowercase();
        let mut text = String::with_capacity(lower.len());
        let mut offsets = Vec::with_capacity(lower.len());
        let mut line = 1usize;
        let mut in_ws = false;
        let mut ws_line = 1usize;
        for ch in lower.chars() {
            if ch.is_whitespace() {
                if !in_ws {
                    in_ws = true;
                    ws_line = line;
                }
                if ch == '\n' {
                    line += 1;
                }
            } else {
                if in_ws {
                    // Emit a single collapsing space if it sits between tokens.
                    if !text.is_empty() {
                        for _ in 0..' '.len_utf8() {
                            offsets.push(ws_line);
                        }
                        text.push(' ');
                    }
                    in_ws = false;
                }
                let start = text.len();
                text.push(ch);
                for _ in start..text.len() {
                    offsets.push(line);
                }
            }
        }
        debug_assert_eq!(text.len(), offsets.len());
        Self { text, offsets }
    }

    /// 1-based source line for the byte at `idx` in [`Self::text`].
    fn line_at(&self, idx: usize) -> usize {
        self.offsets.get(idx).copied().unwrap_or(1)
    }
}

/// A half-open byte span `[start, end)` within a [`NormalizedFile`].
#[derive(Clone, Copy)]
struct Span {
    start: usize,
    end: usize,
}

/// All occurrences of `needle` within `hay`, as byte spans.
fn find_spans(hay: &str, needle: &str) -> Vec<Span> {
    let mut spans = Vec::new();
    if needle.is_empty() {
        return spans;
    }
    let mut from = 0;
    while let Some(rel) = hay[from..].find(needle) {
        let start = from + rel;
        let end = start + needle.len();
        spans.push(Span { start, end });
        from = start + 1; // allow overlapping matches
    }
    spans
}

/// True if `inner` is fully contained within any span in `outer`.
fn covered_by(inner: Span, outer: &[Span]) -> bool {
    outer
        .iter()
        .any(|s| s.start <= inner.start && inner.end <= s.end)
}

/// Scan the given release-facing files for unqualified public-claim phrases.
///
/// Each file is normalized to a single whitespace-collapsed lowercase view (see
/// [`NormalizedFile`]) so a phrase split across a soft-wrap is still detected,
/// while a per-byte line map lets findings report the source line where the
/// phrase starts. For each `claim.blocked.*` entry, every occurrence is a lint
/// error unless either:
///   * that specific occurrence's span falls inside a `claim.safe.*` phrase span
///     (span-based exemption — a safe phrase elsewhere on the line does not
///     exempt a separate over-claim), or
///   * the source line the occurrence starts on is explicitly scoped (see
///     [`SCOPE_MARKERS`]; scope markers remain line-level).
///
/// Findings name the file, line, claim id, and the safe alternative to use.
pub fn scan_docs(m: &Manifest, files: &[ScanInput<'_>]) -> Vec<Finding> {
    let mut out = Vec::new();

    let blocked: Vec<&crate::model::Claim> =
        m.claims.iter().filter(|c| c.kind == "blocked").collect();
    let safe_phrases: Vec<String> = m
        .claims
        .iter()
        .filter(|c| c.kind == "safe")
        .map(|c| normalize(&c.phrase))
        .filter(|p| !p.is_empty())
        .collect();

    for f in files {
        let nf = NormalizedFile::new(f.text);
        if nf.text.is_empty() {
            continue;
        }
        // Spans covered by manifest-backed safe wording — a blocked occurrence is
        // exempt only when *its own* span sits inside one of these.
        let safe_spans: Vec<Span> = safe_phrases
            .iter()
            .flat_map(|p| find_spans(&nf.text, p))
            .collect();
        // Source lines explicitly framing a counter-example/negation. Scope
        // markers stay line-level, keyed by the 1-based source line they sit on.
        let scoped_lines = scoped_source_lines(f.text);

        for c in &blocked {
            let phrase = normalize(&c.phrase);
            for occ in find_spans(&nf.text, &phrase) {
                if covered_by(occ, &safe_spans) {
                    continue;
                }
                let lineno = nf.line_at(occ.start);
                if scoped_lines.contains(&lineno) {
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
                    field: format!("{}:{}", f.path, lineno),
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

/// 1-based source line numbers that contain an explicit scope/negation marker.
fn scoped_source_lines(raw: &str) -> std::collections::HashSet<usize> {
    raw.lines()
        .enumerate()
        .filter_map(|(i, line)| {
            let lower = line.to_lowercase();
            SCOPE_MARKERS
                .iter()
                .any(|mk| lower.contains(mk))
                .then_some(i + 1)
        })
        .collect()
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
