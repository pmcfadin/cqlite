//! Catalog-drift guard for the kit Grafana dashboard (issue #2427).
//!
//! `easy-db-lab-kits/cqlite-flight/dashboards/cqlite-flight.json` visualizes the
//! `cqlite.*` instruments the observability catalog
//! (`cqlite-core/src/observability/catalog.rs`) defines. Panel titles and
//! descriptions embed the DOTTED canonical metric name (e.g.
//! `cqlite.rpc.phase.duration`) as the single machine-checkable reference to what
//! each panel shows — the PromQL `expr`s themselves use the collector-sanitized
//! form (dots→underscores, `_total`/`_seconds`/`_bytes` suffixes), which cannot be
//! reversed to a catalog name unambiguously, so the dotted reference is the
//! anchor.
//!
//! This test extracts every dotted `cqlite.*` token referenced anywhere in the
//! dashboard JSON and asserts each is a real name in
//! [`catalog::ALL_METRICS`]. A renamed/removed/typo'd metric therefore fails
//! CLOSED — the dashboard can never silently reference a phantom instrument.
//! Mirrors the #2426 operator-metrics-doc anti-drift pattern (a committed artifact
//! cross-checked against the catalog at gate time).

use cqlite_core::observability::catalog::{attr, ALL_METRICS};
use std::collections::HashSet;

/// Repo-root-relative path of the kit dashboard.
const DASHBOARD_REL: &str = "easy-db-lab-kits/cqlite-flight/dashboards/cqlite-flight.json";

/// Bounded attribute keys (`cqlite.*`) a dashboard legitimately references in
/// panel `legendFormat`/description text (e.g. `cqlite.rpc.method`). These are
/// dotted `cqlite.*` tokens that are NOT metric names, so the drift check accepts
/// them — but only against this closed list mirrored from `catalog::attr`, so a
/// renamed attribute key is still caught.
const ATTRIBUTE_KEYS: &[&str] = &[
    attr::ERROR_CATEGORY,
    attr::SUBSYSTEM,
    attr::SSTABLE_FORMAT,
    attr::COMPRESSION,
    attr::RESULT,
    attr::LOOKUP_ROUTE,
    attr::ACCESS_PATH,
    attr::PLAN_TYPE,
    attr::RPC_METHOD,
    attr::RPC_STATUS,
    attr::RPC_PHASE,
    attr::FALLBACK_REASON,
    attr::WARM_REFRESH_OUTCOME,
];

/// True when `token` is a namespace-group prefix of a real catalog metric — i.e.
/// some metric name equals `token` or starts with `token + "."`. This admits a
/// row-title group label such as `cqlite.flight.admission` (prefix of
/// `cqlite.flight.admission.in_use`) while still rejecting a typo like
/// `cqlite.flight.admissionx` (no metric starts with `…admissionx.`).
fn is_metric_namespace_prefix(token: &str) -> bool {
    let dotted = format!("{token}.");
    ALL_METRICS
        .iter()
        .any(|m| *m == token || m.starts_with(&dotted))
}

/// Resolve the dashboard path against the repo root (cqlite-core's parent).
fn dashboard_path() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cqlite-core has a repo-root parent")
        .join(DASHBOARD_REL)
}

/// Extract every DOTTED `cqlite.<segment>(.<segment>)+` token from `text`.
///
/// A catalog metric name is `cqlite.` followed by dot-separated lower-snake
/// segments (e.g. `cqlite.flight.admission.wait_seconds`). We greedily consume
/// `[a-z0-9_.]` after the `cqlite.` root and then trim any trailing `.` left by a
/// prose sentence boundary. The collector-sanitized PromQL form uses `_` instead
/// of the leading `cqlite_` dot and so never matches `cqlite.` — those are
/// excluded by construction, exactly as intended (we anchor on the dotted name).
fn dotted_cqlite_names(text: &str) -> HashSet<String> {
    let mut names = HashSet::new();
    for (i, _) in text.match_indices("cqlite.") {
        let tail = &text[i..];
        let token: String = tail
            .chars()
            .take_while(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == '_' || *c == '.')
            .collect();
        // Trim trailing dots (sentence boundary) but keep interior dots.
        let token = token.trim_end_matches('.').to_string();
        // A bare `cqlite.` with no segment is not a metric reference.
        if token.len() > "cqlite.".len() {
            names.insert(token);
        }
    }
    names
}

#[test]
fn dashboard_is_valid_json_with_panels() {
    let raw = std::fs::read_to_string(dashboard_path()).expect("kit dashboard JSON must exist");
    let v: serde_json::Value =
        serde_json::from_str(&raw).expect("kit dashboard must be well-formed JSON");
    let panels = v
        .get("panels")
        .and_then(|p| p.as_array())
        .expect("dashboard must have a panels array");
    // The 0.15 refresh (issue #2427) adds phase/index/admission/warm groups on top
    // of the original 15-panel base RPC set — guard against an accidental revert.
    assert!(
        panels.len() > 15,
        "expected the 0.15-refreshed dashboard to have more than the original 15 panels, got {}",
        panels.len()
    );
}

#[test]
fn every_dashboard_metric_name_exists_in_catalog() {
    let raw = std::fs::read_to_string(dashboard_path()).expect("kit dashboard JSON must exist");
    let catalog: HashSet<&str> = ALL_METRICS.iter().copied().collect();

    let referenced = dotted_cqlite_names(&raw);
    assert!(
        !referenced.is_empty(),
        "expected the dashboard to reference at least one dotted cqlite.* metric name"
    );

    let attrs: HashSet<&str> = ATTRIBUTE_KEYS.iter().copied().collect();

    // A dotted `cqlite.*` token is accepted iff it is an exact metric name, a
    // bounded attribute key, or a namespace-group prefix of a real metric (a row
    // title). Anything else is a renamed/removed/phantom reference — fail CLOSED.
    let mut phantom: Vec<String> = referenced
        .iter()
        .filter(|name| {
            !catalog.contains(name.as_str())
                && !attrs.contains(name.as_str())
                && !is_metric_namespace_prefix(name)
        })
        .cloned()
        .collect();
    phantom.sort();
    assert!(
        phantom.is_empty(),
        "kit dashboard {DASHBOARD_REL} references cqlite.* metric name(s) ABSENT from \
         catalog::ALL_METRICS (renamed/removed/typo'd — fix the dashboard or the catalog): {phantom:?}"
    );

    // At least one referenced token must be an EXACT metric name (not just an
    // attribute key or a group prefix), so the check has real metric coverage.
    assert!(
        referenced.iter().any(|n| catalog.contains(n.as_str())),
        "expected the dashboard to reference at least one exact catalog metric name"
    );
}

#[test]
fn drift_extractor_rejects_a_phantom_name_self_test() {
    // Self-test the guard: a bogus dotted name must be extracted and flagged as
    // absent from the catalog, so a real drift cannot pass silently.
    let catalog: HashSet<&str> = ALL_METRICS.iter().copied().collect();
    let sample = "panel shows cqlite.rpc.phase.duration and a bogus cqlite.does.not.exist metric.";
    let names = dotted_cqlite_names(sample);
    assert!(names.contains("cqlite.rpc.phase.duration"));
    assert!(names.contains("cqlite.does.not.exist"));
    assert!(catalog.contains("cqlite.rpc.phase.duration"));
    assert!(
        !catalog.contains("cqlite.does.not.exist"),
        "self-test bogus name must NOT be in the catalog"
    );
    // Trailing-dot trim: a sentence-final reference resolves to the bare name.
    let trimmed = dotted_cqlite_names("see cqlite.errors.total.");
    assert!(trimmed.contains("cqlite.errors.total"));
}
