//! `cqlite rebuild` — regenerate requested derived SSTable components from a
//! healthy, UNCHANGED `Data.db` (issue #4197, epic #4192). See
//! `openspec/changes/sstable-rebuild/{proposal,design}.md` for the full
//! contract this module implements.
//!
//! Design D1: rebuild does NOT reuse `SSTableWriter::write_partition` (that
//! would rewrite `Data.db` itself). Instead it walks `Data.db` structurally
//! (the same decompression-transparent, decode-error-fails-closed primitives
//! `salvage`/`verify` already use) and drives each requested component's
//! existing LOW-LEVEL writer directly — `IndexWriter`, `SummaryWriter`,
//! `FilterWriter`, `DigestWriter`, `TocWriter`, `crc_writer`, `stats_writer`
//! — never through the higher-level flush/compaction path.
//!
//! # Scope of THIS change (declared, not silent)
//!
//! Full support for BIG (`nb`) `Index.db`/`Summary.db`/`Filter.db`/
//! `Digest.crc32`/`TOC.txt`/`CRC.db`/`Statistics.db`. For a BTI (`da`) input:
//! `Filter.db`/`Digest.crc32`/`TOC.txt`/`Statistics.db` are fully supported
//! (they are format-agnostic); `Summary.db`/`CRC.db` are correctly
//! `skipped_not_applicable` (the BTI format genuinely has neither). BTI's own
//! index (`Partitions.db`/`Rows.db`, [`Component::Index`]'s BTI form) is
//! **NOT implemented in this change** — requesting `index` against a `da`
//! input is a usage error (`Error::UnsupportedFormat`), never a silent
//! wrong-format guess. Tracked as explicit follow-up work under epic #4192
//! (design.md's own per-component table describes the BTI shape; the byte-
//! extent walk + `PartitionsTrieWriter`/`RowsTrieWriter` wiring is
//! substantial enough to warrant its own review pass rather than landing
//! unreviewed inside an already-large change).

mod boundaries;
mod components;
mod decode;
mod simple;
mod statistics;

pub use components::rebuild_components;

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

/// One derived component [`rebuild_components`] can be asked to regenerate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Component {
    /// BIG `Index.db`. Against a BTI (`da`) input this names `Partitions.db`
    /// / `Rows.db` — NOT implemented in this change; see the module doc.
    Index,
    /// BIG `Summary.db`. Not applicable to BTI (`skipped_not_applicable`).
    Summary,
    /// `Filter.db` — applies to both formats.
    Filter,
    /// `Digest.crc32` — applies to both formats.
    Digest,
    /// `TOC.txt` — applies to both formats.
    Toc,
    /// Uncompressed-BIG-only `CRC.db`. Not applicable to compressed input or
    /// BTI (`skipped_not_applicable`).
    Crc,
    /// `Statistics.db` — opt-in (never implied by a bare component omission,
    /// spec R4.4); applies to both formats.
    Statistics,
}

impl Component {
    /// The design.md D5 manifest spelling.
    pub fn manifest_label(self) -> &'static str {
        match self {
            Component::Index => "index",
            Component::Summary => "summary",
            Component::Filter => "filter",
            Component::Digest => "digest",
            Component::Toc => "toc",
            Component::Crc => "crc",
            Component::Statistics => "statistics",
        }
    }

    /// Parse a single component name (case-insensitive, whitespace-trimmed).
    pub fn parse(name: &str) -> std::result::Result<Self, String> {
        match name.trim().to_ascii_lowercase().as_str() {
            "index" => Ok(Component::Index),
            "summary" => Ok(Component::Summary),
            "filter" => Ok(Component::Filter),
            "digest" => Ok(Component::Digest),
            "toc" => Ok(Component::Toc),
            "crc" => Ok(Component::Crc),
            "statistics" => Ok(Component::Statistics),
            other => Err(format!(
                "unknown rebuild component '{other}' (expected one of: index, summary, filter, \
                 digest, toc, crc, statistics)"
            )),
        }
    }

    /// Parse a comma-separated list — the CLI's `--components a,b,c` value.
    pub fn parse_list(spec: &str) -> std::result::Result<Vec<Self>, String> {
        spec.split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(Component::parse)
            .collect()
    }
}

/// Provenance of ONE regenerated field (design.md D5) — never a whole
/// component; only `summary`, `filter` and `statistics` carry per-field
/// classification (every other component is either fully regenerable or
/// refused whole, proposal.md §"What this change must establish").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FieldProvenance {
    /// Derived from an authoritative source other than a hardcoded default —
    /// the original component's own (still-readable) bytes, or the
    /// caller-supplied schema.
    Recovered,
    /// Derived from `Data.db` alone, or fallen back to Cassandra's own
    /// default because the authoritative source was absent. Never a guess —
    /// but not necessarily equal to the ORIGINAL value if the original used
    /// a non-default (e.g. `min_index_interval`, design.md §D2).
    Recomputed,
    /// Genuinely unrecoverable: the source needed to recover it is gone, or
    /// CQLite's own metadata struct carries no field to hold it at all.
    /// Defaulted, and named so — never silently reported as `recovered`.
    Lost,
}

impl FieldProvenance {
    pub fn manifest_label(self) -> &'static str {
        match self {
            FieldProvenance::Recovered => "recovered",
            FieldProvenance::Recomputed => "recomputed",
            FieldProvenance::Lost => "lost",
        }
    }
}

/// Options for one rebuild run (design D5/D3).
///
/// `--in-place`'s temp-file/atomic-rename/audit protocol is gated on #4195
/// (`verify --mode audit` does not exist yet) and is refused entirely at the
/// CLI layer BEFORE `rebuild_components` is ever called (proposal.md
/// "What changes" / tasks.md 4.2) — there is deliberately no `in_place`
/// field here; this struct only ever drives the `--out` path.
#[derive(Debug, Clone)]
pub struct RebuildOptions {
    /// Destination directory for the regenerated components — written as
    /// SIBLING files next to the (untouched) input generation's own name,
    /// unlike `salvage`'s `<out>/<keyspace>/<table>/` nesting (rebuild does
    /// not produce a new generation, so no such nesting applies).
    pub out_dir: PathBuf,
    /// An explicit path to a `Statistics.db`-shaped file to read
    /// `repairedAt`/`pendingRepair`/`isTransient` from, for spec R4.2's
    /// "renamed aside" recovery scenario. `None` (the default) reads the
    /// input's own expected sibling `Statistics.db` path.
    ///
    /// MUST keep the Cassandra `<version>-<generation>-<format>-` filename
    /// convention (e.g. `nb-1-big-Statistics.db`) — `VersionGates::from_path`
    /// derives the version/format gates needed to decode repair metadata
    /// from the FILENAME itself, so an arbitrarily-renamed file (verified
    /// empirically) is read as `lost` rather than `recovered`, even though
    /// its bytes are perfectly intact. Moving it to a different DIRECTORY
    /// with the same filename is the supported "renamed aside" shape.
    pub statistics_recovery_source: Option<PathBuf>,
}

/// Why [`rebuild_components`] refused to write anything (design D3/D5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RefusalReason {
    /// `Data.db` itself could not be trusted as the source of truth (a
    /// chunk-CRC failure, or a partition failed to decode structurally while
    /// walking it) — the remedy always names `salvage` (#4196).
    DataCorrupt,
}

/// A refusal: nothing was written (design D3). Distinct from a usage error
/// (schema unresolvable, an unimplemented BTI-index request, `--in-place`) —
/// those are `Err` returns from [`rebuild_components`]/CLI-level checks, not
/// a populated [`RebuildReport::refused`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Refusal {
    pub reason: RefusalReason,
    pub remedy: String,
    /// The `Data.db` byte offset the refusal was detected at, when known.
    pub offset: Option<u64>,
}

/// A requested component that was not applicable to this input's format, and
/// why (design D5) — e.g. `crc`/`summary` against a BTI (`da`) input.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkippedComponent {
    pub component: String,
    pub reason: String,
}

/// The rebuild manifest (design D5) — the JSON contract; the CLI's text
/// output is a rendering of it, never an independent source of truth.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebuildReport {
    pub input: String,
    pub output: String,
    /// `"nb"` for BIG input, `"da"` for BTI.
    pub format: String,
    pub compressed_input: bool,
    pub requested: Vec<String>,
    pub regenerated: Vec<String>,
    pub skipped_not_applicable: Vec<SkippedComponent>,
    /// Per-component field->provenance-label maps (design D5). Populated
    /// only for `summary`, `filter`, `statistics` — every other component
    /// has nothing to classify (proposal.md).
    pub classification: BTreeMap<String, BTreeMap<String, String>>,
    pub refused: Option<Refusal>,
    /// RFC3339 timestamp of the run.
    pub now: String,
    pub cqlite_version: String,
}

impl RebuildReport {
    /// Text rendering of the manifest (design D5: "the CLI text form is a
    /// rendering of it"). Mirrors salvage's affirmative-zero convention: an
    /// empty `classification`/`skipped_not_applicable` is stated, never
    /// omitted, so an unmeasured run cannot read like a clean one.
    pub fn render_text(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!("rebuild: {}\n", self.input));
        out.push_str(&format!(
            "format: {} (compressed_input={})\n",
            self.format, self.compressed_input
        ));
        out.push_str(&format!("requested: {}\n", self.requested.join(", ")));
        if let Some(refusal) = &self.refused {
            out.push_str(&format!("REFUSED: {}\n", refusal.reason.manifest_label()));
            out.push_str(&format!("remedy: {}\n", refusal.remedy));
            if let Some(offset) = refusal.offset {
                out.push_str(&format!("offset: {offset}\n"));
            }
            return out;
        }
        out.push_str(&format!("regenerated: {}\n", self.regenerated.join(", ")));
        if self.skipped_not_applicable.is_empty() {
            out.push_str("skipped_not_applicable: 0 RECOGNISED\n");
        } else {
            out.push_str("skipped_not_applicable:\n");
            for s in &self.skipped_not_applicable {
                out.push_str(&format!("  - {}: {}\n", s.component, s.reason));
            }
        }
        if self.classification.is_empty() {
            out.push_str("classification: 0 RECOGNISED\n");
        } else {
            out.push_str("classification:\n");
            for (component, fields) in &self.classification {
                out.push_str(&format!("  {component}:\n"));
                for (field, provenance) in fields {
                    out.push_str(&format!("    {field}: {provenance}\n"));
                }
            }
        }
        out
    }
}

impl RefusalReason {
    pub fn manifest_label(self) -> &'static str {
        match self {
            RefusalReason::DataCorrupt => "data-corrupt",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_list_accepts_every_documented_component() {
        let parsed =
            Component::parse_list("index,summary,filter,digest,toc,crc,statistics").unwrap();
        assert_eq!(
            parsed,
            vec![
                Component::Index,
                Component::Summary,
                Component::Filter,
                Component::Digest,
                Component::Toc,
                Component::Crc,
                Component::Statistics,
            ]
        );
    }

    #[test]
    fn parse_list_is_case_insensitive_and_trims_whitespace() {
        let parsed = Component::parse_list(" Index , STATISTICS ").unwrap();
        assert_eq!(parsed, vec![Component::Index, Component::Statistics]);
    }

    #[test]
    fn parse_rejects_unknown_component_naming_it() {
        let err = Component::parse_list("index,bogus").unwrap_err();
        assert!(
            err.contains("bogus"),
            "error must name the bad token: {err}"
        );
    }

    #[test]
    fn render_text_never_omits_empty_classification_or_skips() {
        let report = RebuildReport {
            input: "Data.db".to_string(),
            output: "out".to_string(),
            format: "nb".to_string(),
            compressed_input: false,
            requested: vec!["digest".to_string()],
            regenerated: vec!["digest".to_string()],
            skipped_not_applicable: Vec::new(),
            classification: BTreeMap::new(),
            refused: None,
            now: "2026-01-01T00:00:00Z".to_string(),
            cqlite_version: "0.0.0".to_string(),
        };
        let text = report.render_text();
        assert!(text.contains("skipped_not_applicable: 0 RECOGNISED"));
        assert!(text.contains("classification: 0 RECOGNISED"));
    }

    #[test]
    fn render_text_refused_names_reason_remedy_and_offset() {
        let report = RebuildReport {
            input: "Data.db".to_string(),
            output: "out".to_string(),
            format: "nb".to_string(),
            compressed_input: true,
            requested: vec!["index".to_string()],
            regenerated: Vec::new(),
            skipped_not_applicable: Vec::new(),
            classification: BTreeMap::new(),
            refused: Some(Refusal {
                reason: RefusalReason::DataCorrupt,
                remedy: "cqlite salvage (issue #4196)".to_string(),
                offset: Some(88192),
            }),
            now: "2026-01-01T00:00:00Z".to_string(),
            cqlite_version: "0.0.0".to_string(),
        };
        let text = report.render_text();
        assert!(text.contains("REFUSED: data-corrupt"));
        assert!(text.contains("salvage"));
        assert!(text.contains("offset: 88192"));
        assert!(
            !text.contains("regenerated:"),
            "a refused run must not also print a regenerated line"
        );
    }
}
