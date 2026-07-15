//! Catalog-drift guard for the kit Grafana dashboard (issue #2427).
//!
//! `easy-db-lab-kits/cqlite-flight/dashboards/cqlite-flight.json` visualizes the
//! `cqlite.*` instruments the observability catalog
//! (`cqlite-core/src/observability/catalog.rs`) defines. This test guards the
//! dashboard against catalog drift on TWO fronts:
//!
//! 1. **Dotted references** in panel titles/descriptions embed the canonical
//!    metric name (e.g. `cqlite.rpc.phase.duration`); each must be a real name in
//!    [`catalog::ALL_METRICS`] (or a bounded attribute key / namespace prefix).
//! 2. **PromQL `expr` metric names** — the tokens that ACTUALLY render each panel
//!    — use the Prometheus/OTel-sanitized form (dots→underscores plus type/unit
//!    suffixes: counters `_total`, histograms `_bucket`/`_count`/`_sum`, unit
//!    `_seconds`/`_bytes`). A typo in an `expr` (`…phase_duraton_seconds_bucket`)
//!    leaves the panel silently EMPTY even when the correct dotted name appears in
//!    the title, so we FORWARD-derive the valid sanitized name set from
//!    `operator_metric_docs()` (name + kind + unit — all authoritative, no
//!    hardcoded parallel list) and assert every `cqlite_*` token referenced in
//!    every `expr` is in that set.
//!
//! A renamed/removed/typo'd metric therefore fails CLOSED on either front — the
//! dashboard can never silently reference (or render from) a phantom instrument.
//! Mirrors the #2426 operator-metrics-doc anti-drift pattern (a committed artifact
//! cross-checked against the catalog at gate time).

use cqlite_core::observability::catalog::{attr, unit, ALL_METRICS};
use cqlite_core::observability::operator_docs::{operator_metric_docs, MetricKind};
use std::collections::HashSet;

/// Repo-root-relative path of the kit dashboard.
const DASHBOARD_REL: &str = "easy-db-lab-kits/cqlite-flight/dashboards/cqlite-flight.json";

/// Repo-root-relative path of the kit SUBTREE root. Its presence (a full checkout
/// with the lab kits) is what distinguishes a genuine sparse/minimal checkout
/// (kit absent → SKIP) from real drift/breakage (kit present but the dashboard
/// JSON deleted/renamed → FAIL). Gating the skip on the specific dashboard file
/// would let a delete/rename pass green in a COMPLETE checkout (roborev #2427 r2).
const KIT_ROOT_REL: &str = "easy-db-lab-kits/cqlite-flight";

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

/// The set of Prometheus/OTel-sanitized metric names a catalogued instrument can
/// legitimately expose, forward-derived from its catalog `(name, kind, unit)` —
/// NO hardcoded parallel list, so it stays anti-drift.
///
/// The OTel Prometheus exporter sanitizes a dotted name (dots→underscores) and
/// appends, in order, a UNIT suffix (`s`→`_seconds`, `By`→`_bytes`; other UCUM
/// annotation units like `{row}`/`1` add none) and a TYPE suffix (counters
/// `_total`; histograms expose `_bucket`/`_count`/`_sum`; gauges none). The
/// exporter also de-duplicates — it does NOT re-append a unit/`_total` suffix a
/// name already ends with (e.g. `cqlite.errors.total` stays `…_total`,
/// `…wait_seconds` stays `…_seconds`). We therefore emit a GENEROUS set (the bare
/// stem plus every plausible unit/type variant): permissive on the suffix (so a
/// legit exporter-config difference never false-fails) but STRICT on the stem, so
/// a mistyped metric name (`…phase_duraton_…`) is never in the set.
fn sanitized_variants(name: &str, kind: MetricKind, metric_unit: &str) -> HashSet<String> {
    let base = name.replace('.', "_");
    // Stems: the bare sanitized name, plus a unit-suffixed stem when the unit maps
    // to a Prometheus suffix and the name does not already carry it.
    let mut stems: HashSet<String> = HashSet::new();
    stems.insert(base.clone());
    let unit_suffix = if metric_unit == unit::SECONDS {
        Some("seconds")
    } else if metric_unit == unit::BYTES {
        Some("bytes")
    } else {
        None
    };
    if let Some(u) = unit_suffix {
        if !base.ends_with(u) {
            stems.insert(format!("{base}_{u}"));
        }
    }
    // Type suffixes applied to each stem. Always keep the bare stem too (some
    // exporter configs omit `_total`); the stem is what actually guards typos.
    let mut out: HashSet<String> = HashSet::new();
    for stem in &stems {
        out.insert(stem.clone());
        match kind {
            MetricKind::Gauge => {}
            MetricKind::Counter => {
                if !stem.ends_with("_total") {
                    out.insert(format!("{stem}_total"));
                }
            }
            MetricKind::Histogram => {
                out.insert(format!("{stem}_bucket"));
                out.insert(format!("{stem}_count"));
                out.insert(format!("{stem}_sum"));
            }
        }
    }
    out
}

/// Build the complete set of valid `cqlite_*` Prometheus identifiers a dashboard
/// `expr` may reference: every metric's sanitized variants (from
/// `operator_metric_docs()` — authoritative name+kind+unit) UNION the sanitized
/// bounded attribute keys (used as label selectors / `by(...)` grouping, e.g.
/// `cqlite_rpc_method`). Anti-drift: derived entirely from the catalog.
fn valid_sanitized_names() -> HashSet<String> {
    let mut set = HashSet::new();
    let docs = operator_metric_docs()
        .expect("operator_metric_docs must succeed (every ALL_METRICS entry is annotated)");
    for d in &docs {
        for v in sanitized_variants(d.name, d.kind, d.unit) {
            set.insert(v);
        }
    }
    // Sanitized bounded attribute keys appear as PromQL label names.
    for a in ATTRIBUTE_KEYS {
        set.insert(a.replace('.', "_"));
    }
    set
}

/// Extract every `cqlite_*` Prometheus identifier token referenced in a PromQL
/// `expr`. Metric names appear before `{label…}` selectors and as the leading
/// token of functions (`rate(NAME{…}[5m])`,
/// `histogram_quantile(0.95, sum by(le)(rate(NAME_bucket{…}[5m])))`), and label
/// keys appear inside `{…}` / `by(…)`; all are `[a-zA-Z_][a-zA-Z0-9_]*` tokens.
/// We collect every such token that starts with `cqlite_` and validate each —
/// the point is to catch a mistyped `cqlite_` metric name in an expression.
fn cqlite_expr_tokens(expr: &str) -> HashSet<String> {
    let mut tokens = HashSet::new();
    let bytes = expr.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        let is_ident_start = c.is_ascii_alphabetic() || c == b'_';
        if is_ident_start {
            let start = i;
            while i < bytes.len() {
                let d = bytes[i];
                if d.is_ascii_alphanumeric() || d == b'_' {
                    i += 1;
                } else {
                    break;
                }
            }
            let tok = &expr[start..i];
            if tok.starts_with("cqlite_") {
                tokens.insert(tok.to_string());
            }
        } else {
            i += 1;
        }
    }
    tokens
}

/// Collect every PromQL `expr` string from a dashboard's panels (recursing into
/// nested `panels`, e.g. inside a row), reading each panel target's `expr`.
fn collect_exprs(panels: &serde_json::Value, out: &mut Vec<String>) {
    let Some(arr) = panels.as_array() else {
        return;
    };
    for panel in arr {
        if let Some(targets) = panel.get("targets").and_then(|t| t.as_array()) {
            for t in targets {
                if let Some(expr) = t.get("expr").and_then(|e| e.as_str()) {
                    out.push(expr.to_string());
                }
            }
        }
        if let Some(nested) = panel.get("panels") {
            collect_exprs(nested, out);
        }
    }
}

/// Resolve the kit-subtree root against the repo root (cqlite-core's parent).
fn kit_root_path() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cqlite-core has a repo-root parent")
        .join(KIT_ROOT_REL)
}

/// Loud skip-on-absence — but ONLY for a genuine sparse checkout.
///
/// The kit dashboard lives under `easy-db-lab-kits/` which is NOT present in a
/// sparse/minimal checkout that has cqlite-core alone. This test runs under the
/// `core-tests` gate component (all integration tests), so it must SKIP — not
/// FAIL — when the WHOLE kit subtree is absent.
///
/// Crucially, the skip is gated on the KIT ROOT directory, not on the specific
/// dashboard file (roborev #2427 r2): if the kit subtree IS present but the
/// expected dashboard JSON has been deleted/renamed, that is real drift/breakage
/// in a complete checkout and must FAIL — panicking loudly — never a silent skip
/// that defeats the artifact-presence + panel-count protection. Mirrors the
/// repo's local-only-fixture skip-on-presence convention (skip only when the
/// whole fixture family is unavailable, fail on 0-when-present).
///
/// Returns `None` (caller returns early, prints why) on a genuine sparse checkout;
/// `Some(raw)` with the file contents otherwise; panics on the present-kit,
/// missing-dashboard drift case.
fn read_dashboard_or_skip() -> Option<String> {
    let kit_root = kit_root_path();
    if !kit_root.exists() {
        eprintln!(
            "SKIP: kit subtree absent ({}) — sparse checkout without easy-db-lab-kits/; \
             the kit-dashboard-drift gate component enforces presence in full checkouts",
            kit_root.display()
        );
        return None;
    }
    let path = dashboard_path();
    assert!(
        path.exists(),
        "FAIL: kit subtree {} IS present but the expected dashboard JSON is MISSING ({}) — \
         a deleted/renamed dashboard in a complete checkout is drift/breakage, not a sparse \
         checkout. Restore the dashboard at {DASHBOARD_REL} or update this test's path.",
        kit_root.display(),
        path.display()
    );
    Some(std::fs::read_to_string(&path).expect("kit dashboard JSON must be readable when present"))
}

#[test]
fn dashboard_is_valid_json_with_panels() {
    let Some(raw) = read_dashboard_or_skip() else {
        return;
    };
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
    let Some(raw) = read_dashboard_or_skip() else {
        return;
    };
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
fn every_expr_metric_name_is_a_valid_sanitized_catalog_name() {
    // FINDING 1 (roborev #2427 r2): the exprs are what actually RENDER each panel.
    // A typo in an expr metric name (`cqlite_rpc_phase_duraton_seconds_bucket`)
    // leaves the panel silently EMPTY even though the correct dotted name still
    // appears in the title — so we validate the PromQL expr tokens against the
    // forward-derived sanitized-name set, not just the titles/descriptions.
    let Some(raw) = read_dashboard_or_skip() else {
        return;
    };
    let v: serde_json::Value =
        serde_json::from_str(&raw).expect("kit dashboard must be well-formed JSON");
    let mut exprs = Vec::new();
    collect_exprs(
        v.get("panels").expect("dashboard must have a panels array"),
        &mut exprs,
    );
    assert!(
        !exprs.is_empty(),
        "expected the dashboard to have at least one panel target with a PromQL expr"
    );

    let valid = valid_sanitized_names();

    // Every `cqlite_*` token referenced in any expr must be a valid sanitized
    // metric name (or attribute label) — anything else is a mistyped/renamed
    // reference that renders an EMPTY panel. Fail CLOSED, naming the offenders.
    let mut invalid: Vec<String> = Vec::new();
    let mut referenced_any_metric = false;
    for expr in &exprs {
        for tok in cqlite_expr_tokens(expr) {
            if valid.contains(&tok) {
                referenced_any_metric = true;
            } else {
                invalid.push(tok);
            }
        }
    }
    invalid.sort();
    invalid.dedup();
    assert!(
        invalid.is_empty(),
        "kit dashboard {DASHBOARD_REL} references cqlite_* PromQL metric/label name(s) that are \
         NOT valid sanitized forms of any catalog metric or bounded attribute (a typo/rename \
         renders the panel EMPTY — fix the expr or the catalog): {invalid:?}"
    );
    assert!(
        referenced_any_metric,
        "expected the dashboard exprs to reference at least one valid cqlite_* sanitized name"
    );
}

#[test]
fn expr_validator_rejects_a_typo_in_an_expr_metric_name_negative_test() {
    // FINDING 1 negative test (roborev #2427 r2): corrupt ONLY an expr's metric
    // name — leaving the correct dotted name in the title/description — and assert
    // the guard flags it. This is precisely the empty-panel bug the title-only
    // check missed: the correct dotted name elsewhere no longer rescues a typo'd
    // expr. Uses a synthetic expr so the test never depends on the live dashboard.
    let valid = valid_sanitized_names();

    // The correct sanitized name IS in the valid set…
    assert!(
        valid.contains("cqlite_rpc_phase_duration_seconds_bucket"),
        "the correct histogram bucket name must be a valid sanitized variant"
    );
    // …but a one-character typo ("duraton") is NOT — so the panel would be empty.
    let typo_expr = "histogram_quantile(0.95, sum(rate(cqlite_rpc_phase_duraton_seconds_bucket\
                     {cluster=~\"$cluster\"}[5m])) by (le, cqlite_rpc_phase))";
    let mut invalid: Vec<String> = cqlite_expr_tokens(typo_expr)
        .into_iter()
        .filter(|t| !valid.contains(t))
        .collect();
    invalid.sort();
    assert_eq!(
        invalid,
        vec!["cqlite_rpc_phase_duraton_seconds_bucket".to_string()],
        "the expr validator must flag exactly the typo'd metric name (and nothing else)"
    );

    // Sanity: a counter's `_total` and a histogram's `_bucket`/`_count`/`_sum` are
    // all accepted (forward-derived from kind), a bare gauge name is accepted, and
    // a sanitized attribute label is accepted.
    assert!(
        valid.contains("cqlite_rpc_requests_total"),
        "counter _total"
    );
    assert!(
        valid.contains("cqlite_rpc_duration_seconds_count"),
        "histogram _count"
    );
    assert!(
        valid.contains("cqlite_rpc_duration_seconds_sum"),
        "histogram _sum"
    );
    assert!(valid.contains("cqlite_rpc_in_flight"), "bare gauge name");
    assert!(
        valid.contains("cqlite_rpc_bytes_total"),
        "counter with By unit + _total"
    );
    assert!(
        valid.contains("cqlite_rpc_method"),
        "sanitized attribute label"
    );
    // A name that already ends in `_total` is not double-suffixed.
    assert!(
        valid.contains("cqlite_errors_total"),
        "errors.total counter"
    );
    assert!(
        !valid.contains("cqlite_errors_total_total"),
        "must not double-append _total to a name already ending in _total"
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
