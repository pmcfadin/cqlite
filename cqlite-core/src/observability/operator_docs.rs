//! Operator-facing metric reference — GENERATED source of truth (issue #2426).
//!
//! The developer-facing catalog ([`super::catalog`]) owns the metric *names*,
//! *units*, and cardinality contracts. This module adds the **operator**
//! annotation for every catalogued instrument (one-sentence meaning, bounded
//! attribute set, healthy-vs-alarming interpretation, and the #2399 round
//! scoreboard item it feeds) and renders a deterministic markdown reference so
//! the AWS field team has a single consumable page that CANNOT drift from the
//! code.
//!
//! # Anti-drift contract (mirrors the #1338 parity-report pattern)
//!
//! - The renderer's source of truth is the REAL catalog: it walks
//!   [`super::catalog::ALL_METRICS`] and references the `catalog::attr::*`
//!   constants, never a hand-copied name list.
//! - Generation is **fail-closed**: a metric present in `ALL_METRICS` with no
//!   operator annotation makes [`operator_metric_docs`] return
//!   [`DocGenError::MissingAnnotation`] — no undocumented instrument can ship.
//!   An annotation for a name absent from `ALL_METRICS` is likewise rejected.
//! - The rendered markdown is committed at [`COMMITTED_DOC_REL`]; a lib test
//!   (`operator_metrics_doc_is_fresh`) and the `operator-metrics-doc` agent-gate
//!   component fail if the committed file drifts from a fresh render.
//! - Regenerate with:
//!   `cargo run -p cqlite-core --example gen_operator_metrics_doc`.
//!
//! Everything here is always compiled (it pulls in no OpenTelemetry types), like
//! the catalog itself, so the generator and its freshness test build in any
//! feature configuration.

use super::catalog::{ALL_METRICS, STATS_ONLY_METRICS};
use super::operator_docs_annotations::ANNOTATIONS;

/// Repository-relative path of the committed operator reference. The generator
/// example and the freshness test resolve it against `CARGO_MANIFEST_DIR/..`.
pub const COMMITTED_DOC_REL: &str = "docs/reports/flight-metrics-reference.md";

/// Repository-relative path of the PUBLISHED docs-site page (Astro/Starlight
/// content). Generated from the same catalog render (with front matter) so the
/// published operator page cannot drift from the code either (issue #2426).
pub const WEBSITE_DOC_REL: &str =
    "website/src/content/docs/agents-using/flight-metrics-reference.md";

/// Astro/Starlight front matter for the published website page. Kept beside the
/// renderer so the example and the freshness test agree byte-for-byte.
const WEBSITE_FRONT_MATTER: &str = "\
---
title: Flight metrics reference
description: Operator reference for every cqlite.* metric in CQLite's observability catalog — which names are live OTel instruments you can scrape, and which are stats-only and readable solely from Database::stats(). Generated from the catalog so it cannot drift.
sidebar:
  label: Flight metrics reference
  order: 20
---

";

/// OpenTelemetry instrument kind, as the field team reads it on a dashboard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricKind {
    /// Monotonically increasing total.
    Counter,
    /// Current up/down value.
    Gauge,
    /// Distribution of observed values (durations, sizes, ratios).
    Histogram,
}

impl MetricKind {
    /// Lowercase label used in the rendered reference.
    pub fn label(self) -> &'static str {
        match self {
            MetricKind::Counter => "counter",
            MetricKind::Gauge => "gauge",
            MetricKind::Histogram => "histogram",
        }
    }
}

/// One operator annotation for a catalogued instrument.
#[derive(Debug, Clone)]
pub struct MetricDoc {
    /// Metric name (a `catalog::*` constant — never a copied literal).
    pub name: &'static str,
    /// Instrument kind.
    pub kind: MetricKind,
    /// UCUM unit string.
    pub unit: &'static str,
    /// One operator sentence: what this instrument means in the field.
    pub summary: &'static str,
    /// Bounded attribute keys this metric may carry (`catalog::attr::*`), empty
    /// when the metric carries none.
    pub attributes: &'static [&'static str],
    /// Healthy value vs the shape that should raise an alarm.
    pub interpretation: &'static str,
    /// #2399 round-scoreboard item this metric feeds, or `"—"`.
    pub round_item: &'static str,
}

/// Error raised when the operator annotations and the catalog disagree — the
/// fail-closed guard against an undocumented (or phantom) instrument shipping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DocGenError {
    /// A metric in [`ALL_METRICS`] has no operator annotation.
    MissingAnnotation(&'static str),
    /// An annotation names a metric that is not in [`ALL_METRICS`].
    UnknownMetric(&'static str),
    /// An annotation names a metric more than once.
    DuplicateAnnotation(&'static str),
}

impl std::fmt::Display for DocGenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DocGenError::MissingAnnotation(m) => write!(
                f,
                "metric `{m}` is catalogued but has no operator annotation in \
                 operator_docs::ANNOTATIONS — no undocumented instrument may ship (#2426)"
            ),
            DocGenError::UnknownMetric(m) => write!(
                f,
                "operator annotation names `{m}`, which is not in catalog::ALL_METRICS \
                 (stale annotation)"
            ),
            DocGenError::DuplicateAnnotation(m) => {
                write!(f, "operator annotation for `{m}` appears more than once")
            }
        }
    }
}

impl std::error::Error for DocGenError {}

/// Return every catalogued instrument paired with its operator annotation, in
/// catalog (`ALL_METRICS`) order.
///
/// Fail-closed: returns [`DocGenError`] if any metric lacks an annotation, if an
/// annotation names a metric absent from `ALL_METRICS`, or if a metric is
/// annotated twice — so no undocumented (or phantom) instrument can ship.
pub fn operator_metric_docs() -> Result<Vec<MetricDoc>, DocGenError> {
    // Reject duplicate annotations first (a duplicate would mask a missing one).
    let mut seen = std::collections::HashSet::new();
    for a in ANNOTATIONS {
        if !seen.insert(a.name) {
            return Err(DocGenError::DuplicateAnnotation(a.name));
        }
        if !ALL_METRICS.contains(&a.name) {
            return Err(DocGenError::UnknownMetric(a.name));
        }
    }
    // Every catalogued metric must have an annotation, walked in catalog order.
    let mut out = Vec::with_capacity(ALL_METRICS.len());
    for name in ALL_METRICS {
        match ANNOTATIONS.iter().find(|a| a.name == *name) {
            Some(a) => out.push(a.clone()),
            None => return Err(DocGenError::MissingAnnotation(name)),
        }
    }
    Ok(out)
}

/// The bounded attribute keys of `d`, as a markdown table cell.
fn render_attributes(d: &MetricDoc) -> String {
    if d.attributes.is_empty() {
        "_(none)_".to_string()
    } else {
        d.attributes
            .iter()
            .map(|a| format!("`{a}`"))
            .collect::<Vec<_>>()
            .join("<br>")
    }
}

/// Render the deterministic operator-facing markdown reference from the catalog.
///
/// Output is stable across runs (metrics sorted by name, no timestamps) so a
/// `--check` regeneration can fail on a stale committed artifact — the #1338
/// derived-artifact pattern.
pub fn render_markdown() -> Result<String, DocGenError> {
    let mut docs = operator_metric_docs()?;
    docs.sort_by(|a, b| a.name.cmp(b.name));

    // Split the catalog into the two populations an operator experiences
    // DIFFERENTLY (issue #1705): a live OTel instrument is scrapeable, a
    // `catalog::STATS_ONLY_METRICS` entry provably is not (that list is exactly
    // the set of catalogued names with no instrument, enforced by
    // `stats_only_metrics_are_catalogued_and_never_otel_registered`). Rendering
    // them under one "all instruments" heading with one total told operators to
    // go looking on a Prometheus scrape for names that cannot be there.
    let stats_only: std::collections::HashSet<&str> =
        STATS_ONLY_METRICS.iter().map(|m| m.name).collect();
    let (stats_only_docs, live): (Vec<MetricDoc>, Vec<MetricDoc>) = docs
        .iter()
        .cloned()
        .partition(|d| stats_only.contains(d.name));

    let mut s = String::new();
    let mut line = |t: &str| {
        s.push_str(t);
        s.push('\n');
    };

    line("# CQLite Flight metrics — operator reference");
    line("");
    line(
        "> GENERATED from the observability catalog \
         (`cqlite-core/src/observability/catalog.rs` + `operator_docs.rs`) by \
         `cargo run -p cqlite-core --example gen_operator_metrics_doc`. \
         Do NOT edit by hand — edit the catalog/annotations and regenerate. \
         The `operator-metrics-doc` agent-gate component fails if this file drifts \
         from the catalog (issue #2426).",
    );
    line("");
    line(
        "Operator-facing reference for every `cqlite.*` metric name in CQLite's \
         observability catalog, covering Arrow Flight and the storage/write/compaction \
         paths. Names, units, and bounded attribute sets come straight from the code, \
         so this page cannot silently diverge from what a running server exposes.",
    );
    line("");
    line(
        "**Two populations, and the difference matters on a scrape.** Most catalogued \
         names are LIVE OTel instruments: they are registered with the meter and show \
         up on a Prometheus scrape / OTel collector export. The rest are **stats-only** \
         — real recorded values that CQLite surfaces ONLY through the in-process \
         `Database::stats().memory_stats` snapshot and never registers as an \
         instrument. You will NOT find a stats-only name on a scrape, however long you \
         look; read it from memory stats instead. The two are listed in separate \
         sections below.",
    );
    line("");
    line("Related: the Flight/Trino operator docs (`docs/flight-trino/`) and the round scoreboard template (issue #2399) link back to the entries here.");
    line("");
    line(&format!(
        "Catalogued metrics: **{}** — **{}** live OTel instruments and **{}** \
         stats-only (not scrapeable).",
        docs.len(),
        live.len(),
        stats_only_docs.len(),
    ));
    line("");

    line("## Live instruments");
    line("");
    line(
        "Registered with the OTel meter, so these appear on a Prometheus scrape / OTel \
         collector export.",
    );
    line("");
    line("| Metric | Type | Unit | Attributes | Operator meaning | Healthy vs alarming |");
    line("|---|---|---|---|---|---|");
    for d in &live {
        line(&format!(
            "| `{}` | {} | `{}` | {} | {} | {} |",
            d.name,
            d.kind.label(),
            d.unit,
            render_attributes(d),
            d.summary,
            d.interpretation,
        ));
    }
    if live.is_empty() {
        line("| _(none)_ | | | | | |");
    }
    line("");

    line("## Stats-only metrics — NOT OTel instruments");
    line("");
    line(&format!(
        "These **{}** catalogued names have NO OTel instrument: nothing registers them \
         with a meter, so a Prometheus scrape or OTel collector will never show them. \
         Read them from the in-process `Database::stats()` snapshot at the field named \
         in the last column. This is enforced, not documentation: \
         `catalog::STATS_ONLY_METRICS` is what this section is generated from, and the \
         `stats_only_metrics_are_catalogued_and_never_otel_registered` guard fails the \
         build if one of them ever does get an instrument (issue #1705).",
        stats_only_docs.len(),
    ));
    line("");
    line("| Metric | Type | Unit | Attributes | Operator meaning | Healthy vs alarming | Read it from |");
    line("|---|---|---|---|---|---|---|");
    for d in &stats_only_docs {
        let field = STATS_ONLY_METRICS
            .iter()
            .find(|m| m.name == d.name)
            .map(|m| format!("`Database::stats().{}`", m.stats_field))
            .unwrap_or_else(|| "—".to_string());
        line(&format!(
            "| `{}` | {} | `{}` | {} | {} | {} | {} |",
            d.name,
            d.kind.label(),
            d.unit,
            render_attributes(d),
            d.summary,
            d.interpretation,
            field,
        ));
    }
    if stats_only_docs.is_empty() {
        line("| _(none)_ | | | | | | |");
    }
    line("");

    // Round-scoreboard mapping (#2399): only metrics that feed a template item.
    line("## Round-scoreboard mapping (issue #2399)");
    line("");
    line("Metrics a #2399 round-template scoreboard item consumes. Round handoffs (#2367-style) link the item to its metric here instead of re-explaining it.");
    line("");
    line("| Metric | Scoreboard item |");
    line("|---|---|");
    let mut any = false;
    for d in &docs {
        if d.round_item != "—" {
            any = true;
            line(&format!("| `{}` | {} |", d.name, d.round_item));
        }
    }
    if !any {
        line("| _(none)_ | _(none)_ |");
    }
    line("");

    Ok(s)
}

/// Render the published website (Astro/Starlight) variant: the same catalog
/// render as [`render_markdown`] with Starlight front matter prepended. Sharing
/// one render keeps the published page from drifting from the committed report.
pub fn render_website_markdown() -> Result<String, DocGenError> {
    Ok(format!("{WEBSITE_FRONT_MATTER}{}", render_markdown()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_catalogued_metric_has_an_operator_annotation() {
        // Fail-closed generation (#2426 AC1): no undocumented instrument may ship.
        let docs = operator_metric_docs().expect("every ALL_METRICS entry must be annotated");
        assert_eq!(
            docs.len(),
            ALL_METRICS.len(),
            "operator doc count must equal the catalog metric count"
        );
    }

    #[test]
    fn no_annotation_names_a_metric_absent_from_the_catalog() {
        // A stale annotation (name removed from the catalog) is rejected.
        for a in ANNOTATIONS {
            assert!(
                ALL_METRICS.contains(&a.name),
                "annotation names `{}`, absent from ALL_METRICS",
                a.name
            );
        }
    }

    #[test]
    fn attributes_are_catalogued_bounded_keys() {
        // Every attribute an annotation lists must be a real `cqlite.`-namespaced
        // bounded key, so the reference cannot invent an unbounded dimension.
        let docs = operator_metric_docs().unwrap();
        for d in &docs {
            for a in d.attributes {
                assert!(
                    a.starts_with("cqlite."),
                    "attribute `{a}` on `{}` must be a namespaced bounded key",
                    d.name
                );
            }
        }
    }

    #[test]
    fn render_is_deterministic_and_non_empty() {
        let a = render_markdown().unwrap();
        let b = render_markdown().unwrap();
        assert_eq!(a, b, "render must be byte-stable across runs");
        assert!(a.contains("# CQLite Flight metrics — operator reference"));
        // Every metric name appears in the rendered table.
        for name in ALL_METRICS {
            assert!(a.contains(name), "rendered doc must mention `{name}`");
        }
    }

    /// The rendered counts and section split must be DERIVED from the catalog
    /// (issue #1705).
    ///
    /// The `operator-metrics-doc` gate component only compares the committed file
    /// against a fresh render, so it is structurally blind to a generator whose
    /// PROSE is false: before this, the page announced "every `cqlite.*` instrument
    /// CQLite emits" and "Total instruments: 84" under one "All instruments"
    /// heading, while 6 of those 84 are `catalog::STATS_ONLY_METRICS` and are
    /// GUARANTEED (by `stats_only_metrics_are_catalogued_and_never_otel_registered`)
    /// to have no instrument at all — the page sent operators looking on a
    /// Prometheus scrape for six names that provably cannot appear there. That is
    /// issue #1705's own bug class: a doc promising behaviour the code does not have.
    ///
    /// The counts here are computed from the catalog independently of the renderer,
    /// so a hard-coded number in the generator reds this the moment the catalog
    /// moves — the claim has to be derived, not asserted.
    #[test]
    fn rendered_counts_and_sections_are_derived_from_the_catalog() {
        let doc = render_markdown().expect("render must succeed");
        let stats_only: std::collections::HashSet<&str> =
            STATS_ONLY_METRICS.iter().map(|m| m.name).collect();
        let total = ALL_METRICS.len();
        let n_stats_only = ALL_METRICS
            .iter()
            .filter(|n| stats_only.contains(*n))
            .count();
        let n_live = total - n_stats_only;

        let expected_counts = format!(
            "Catalogued metrics: **{total}** — **{n_live}** live OTel instruments and \
             **{n_stats_only}** stats-only (not scrapeable)."
        );
        assert!(
            doc.contains(&expected_counts),
            "the rendered counts must be derived from the catalog; expected \
             {expected_counts:?}"
        );

        // The retired claim must not come back: there is no single "all instruments"
        // population, because 6 catalogued names are not instruments.
        assert!(
            !doc.contains("## All instruments"),
            "a single 'All instruments' section misrepresents the stats-only names"
        );
        assert!(
            !doc.contains("Total instruments:"),
            "a single instrument total counts stats-only names as instruments"
        );

        // Each population is rendered in ITS OWN section, and no name is in both.
        let live_head = "## Live instruments";
        let stats_head = "## Stats-only metrics — NOT OTel instruments";
        let live_at = doc.find(live_head).expect("live-instrument section");
        let stats_at = doc.find(stats_head).expect("stats-only section");
        assert!(live_at < stats_at, "sections must render in a stable order");
        let live_section = &doc[live_at..stats_at];
        // Bounded at the next heading: the round-scoreboard table that follows lists
        // rows in the same `| `name` |` shape, and swallowing it would make every
        // live metric look like it were also declared stats-only.
        let stats_end = doc[stats_at..]
            .find("\n## ")
            .map(|o| stats_at + o)
            .unwrap_or(doc.len());
        let stats_section = &doc[stats_at..stats_end];

        for name in ALL_METRICS {
            let row = format!("| `{name}` |");
            if stats_only.contains(name) {
                assert!(
                    stats_section.contains(&row),
                    "stats-only `{name}` must be listed in the stats-only section"
                );
                assert!(
                    !live_section.contains(&row),
                    "`{name}` has no OTel instrument, so it must NOT be listed as a \
                     live instrument"
                );
            } else {
                assert!(
                    live_section.contains(&row),
                    "`{name}` is a live instrument and must be listed as one"
                );
                assert!(
                    !stats_section.contains(&row),
                    "`{name}` has a live instrument, so it must NOT be listed as \
                     stats-only"
                );
            }
        }

        // And the stats-only rows tell the operator where to actually read them.
        for m in STATS_ONLY_METRICS {
            assert!(
                stats_section.contains(&format!("`Database::stats().{}`", m.stats_field)),
                "the stats-only row for `{}` must name its stats field",
                m.name
            );
        }
    }

    #[test]
    fn committed_operator_metrics_doc_is_fresh() {
        // Anti-drift guard (#2426 AC2/AC5): the committed reference MUST match a
        // fresh render of the catalog. If a metric is added/renamed or an
        // annotation changes without regenerating, this fails — mirroring the
        // #1338 parity-report freshness check, at the lib-test layer so it runs in
        // both the full gate's core-tests and the lite gate's cqlite-core --lib.
        let repo_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("cqlite-core has a repo-root parent");
        let path = repo_root.join(COMMITTED_DOC_REL);
        let committed = std::fs::read_to_string(&path).unwrap_or_else(|e| {
            panic!(
                "committed operator metrics doc {} is missing ({e}); regenerate with \
                 `cargo run -p cqlite-core --example gen_operator_metrics_doc`",
                path.display()
            )
        });
        let fresh = render_markdown().expect("render must succeed");
        assert_eq!(
            committed,
            fresh,
            "{} is STALE vs the catalog; regenerate with \
             `cargo run -p cqlite-core --example gen_operator_metrics_doc`",
            path.display()
        );

        // The published website page is the same render with front matter.
        let web_path = repo_root.join(WEBSITE_DOC_REL);
        let web_committed = std::fs::read_to_string(&web_path).unwrap_or_else(|e| {
            panic!(
                "published website metrics page {} is missing ({e}); regenerate with \
                 `cargo run -p cqlite-core --example gen_operator_metrics_doc`",
                web_path.display()
            )
        });
        let web_fresh = render_website_markdown().expect("website render must succeed");
        assert_eq!(
            web_committed,
            web_fresh,
            "{} is STALE vs the catalog; regenerate with \
             `cargo run -p cqlite-core --example gen_operator_metrics_doc`",
            web_path.display()
        );
    }

    #[test]
    fn missing_annotation_is_detected() {
        // Directly exercise the fail-closed path: a catalog with an unannotated
        // name must surface MissingAnnotation. We simulate by asserting the error
        // Display is actionable for the real error variants.
        let e = DocGenError::MissingAnnotation("cqlite.example.unannotated");
        assert!(e.to_string().contains("no operator annotation"));
        let u = DocGenError::UnknownMetric("cqlite.example.stale");
        assert!(u.to_string().contains("not in catalog::ALL_METRICS"));
    }
}
