//! Catalog-drift guard for the kit Grafana dashboard (issue #2427).
//!
//! `easy-db-lab-kits/cqlite-flight/dashboards/cqlite-flight.json` visualizes the
//! `cqlite.*` instruments the observability catalog
//! (`cqlite-core/src/observability/catalog.rs`) defines. This test guards the
//! dashboard against catalog drift on TWO fronts:
//!
//! 1. **Dotted references** in panel titles/descriptions embed the canonical
//!    metric name (e.g. `cqlite.rpc.phase.duration`); each must be an EXACT name
//!    in [`catalog::ALL_METRICS`], an EXACT bounded attribute key, or an explicit
//!    `.*` wildcard group ref (`cqlite.flight.admission.*`) — a bare namespace
//!    prefix that is none of those FAILS (roborev #2427 r3, F2).
//! 2. **PromQL `expr` metric names** — the tokens that ACTUALLY render each panel
//!    — use the Prometheus/OTel-sanitized form (dots→underscores plus the EXACT
//!    unit+type suffixes the collector emits: a counter is `<stem>_total`, a
//!    seconds histogram is `<stem>_seconds_{bucket,count,sum}`, a gauge is the
//!    bare unit-suffixed stem). A typo (`…phase_duraton_seconds_bucket`) OR a
//!    bare/mis-suffixed form (`cqlite_rpc_requests` missing `_total`,
//!    `cqlite_rpc_phase_duration_bucket` missing `_seconds`) leaves the panel
//!    silently EMPTY even when the correct dotted name appears in the title, so we
//!    FORWARD-derive the EXACT emitted-name set (NOT a permissive superset) from
//!    `operator_metric_docs()` (name + kind + unit — all authoritative, no
//!    hardcoded parallel list) and assert every `cqlite_*` token referenced in
//!    every `expr` is in that set (roborev #2427 r3, F1). Attribute-label tokens
//!    are tracked in a SEPARATE set so referencing only an attribute never counts
//!    as referencing a metric (roborev #2427 r3, F3).
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

/// True when `token` is an EXPLICIT `.*` wildcard group reference whose namespace
/// covers at least one real catalog metric — e.g. `cqlite.flight.admission.*`
/// (covers `cqlite.flight.admission.in_use`, …). ONLY the explicit-wildcard form
/// is admitted as a group ref (roborev #2427 r3, F2): a BARE dotted prefix such as
/// `cqlite.flight.admission` (no trailing `.*`) is NOT a valid reference, so a
/// phantom that merely happens to be a prefix of a real namespace can no longer
/// pass. The stem must still cover a real metric, so `cqlite.does.not.exist.*` is
/// rejected.
fn is_wildcard_group_ref(token: &str) -> bool {
    let Some(stem) = token.strip_suffix(".*") else {
        return false;
    };
    let dotted = format!("{stem}.");
    ALL_METRICS
        .iter()
        .any(|m| *m == stem || m.starts_with(&dotted))
}

/// Resolve the dashboard path against the repo root (cqlite-core's parent).
fn dashboard_path() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cqlite-core has a repo-root parent")
        .join(DASHBOARD_REL)
}

/// Extract every DOTTED `cqlite.<segment>(.<segment>)+` token from `text`,
/// preserving an explicit trailing `.*` wildcard group marker.
///
/// A catalog metric name is `cqlite.` followed by dot-separated lower-snake
/// segments (e.g. `cqlite.flight.admission.wait_seconds`). We greedily consume
/// `[a-z0-9_.*]` after the `cqlite.` root, then trim a trailing prose dot while
/// keeping a genuine `.*` wildcard group ref (`cqlite.flight.admission.*`). The
/// collector-sanitized PromQL form uses `_` instead of the leading `cqlite_` dot
/// and so never matches `cqlite.` — those are excluded by construction, exactly
/// as intended (we anchor on the dotted name).
fn dotted_cqlite_names(text: &str) -> HashSet<String> {
    let mut names = HashSet::new();
    for (i, _) in text.match_indices("cqlite.") {
        let tail = &text[i..];
        let token: String = tail
            .chars()
            .take_while(|c| {
                c.is_ascii_lowercase() || c.is_ascii_digit() || *c == '_' || *c == '.' || *c == '*'
            })
            .collect();
        // Preserve an explicit `.*` wildcard group ref; otherwise trim any
        // trailing `.`/`*` left by a prose sentence boundary (keep interior dots).
        let token = if token.ends_with(".*") {
            token
        } else {
            token.trim_end_matches(['.', '*']).to_string()
        };
        // A bare `cqlite.` with no segment is not a metric reference.
        if token.len() > "cqlite.".len() {
            names.insert(token);
        }
    }
    names
}

/// The EXACT Prometheus/OTel-sanitized metric names a catalogued instrument
/// exposes, forward-derived from its catalog `(name, kind, unit)` — NO hardcoded
/// parallel list (anti-drift) and NO permissive superset (roborev #2427 r3, F1):
/// the set holds ONLY the names the collector actually emits, so a dashboard that
/// uses a bare/mis-suffixed form (which would render an EMPTY panel) is rejected.
///
/// The OTel Prometheus exporter sanitizes a dotted name (dots→underscores), then
/// appends, in order, a UNIT suffix (`s`→`_seconds`, `By`→`_bytes`; other UCUM
/// annotation units like `{row}`/`1` add none) and a TYPE suffix. It also
/// de-duplicates — it does NOT re-append a unit/type suffix a name already ends
/// with. The exact emitted name(s) per kind:
/// - **Counter** → the single `<stem>_total` (bare name is NEVER scraped; e.g.
///   `cqlite.rpc.requests` → `cqlite_rpc_requests_total`,
///   `cqlite.rpc.bytes` [`By`] → `cqlite_rpc_bytes_total` — the `By`→`_bytes` is
///   de-duped against the name's existing `bytes` stem, matching the
///   field-verified base-RPC exprs). A name already ending `_total` stays as-is.
/// - **Histogram** → three series `<stem>_bucket`, `<stem>_count`, `<stem>_sum`,
///   with the unit stem folded in first (`s` → `<stem>_seconds_{bucket,count,sum}`,
///   seconds BEFORE the `_bucket`/`_count`/`_sum`).
/// - **Gauge** → the bare unit-suffixed `<stem>` (no type suffix).
///
/// The STEM is dedup-guarded so a name already carrying its unit/type suffix is
/// never double-suffixed (`cqlite.errors.total` stays `…_total`;
/// `cqlite.flight.admission.wait_seconds` stays `…_seconds_bucket`).
fn sanitized_variants(name: &str, kind: MetricKind, metric_unit: &str) -> HashSet<String> {
    let base = name.replace('.', "_");
    // Fold the UNIT suffix into the stem first (de-duped: never re-append a suffix
    // the name already ends with).
    let unit_suffix = if metric_unit == unit::SECONDS {
        Some("seconds")
    } else if metric_unit == unit::BYTES {
        Some("bytes")
    } else {
        None
    };
    let stem = match unit_suffix {
        Some(u) if !base.ends_with(u) => format!("{base}_{u}"),
        _ => base,
    };
    // Then the TYPE suffix — the EXACT emitted series, no bare-stem fallback.
    let mut out: HashSet<String> = HashSet::new();
    match kind {
        MetricKind::Gauge => {
            out.insert(stem);
        }
        MetricKind::Counter => {
            if stem.ends_with("_total") {
                out.insert(stem);
            } else {
                out.insert(format!("{stem}_total"));
            }
        }
        MetricKind::Histogram => {
            out.insert(format!("{stem}_bucket"));
            out.insert(format!("{stem}_count"));
            out.insert(format!("{stem}_sum"));
        }
    }
    out
}

/// The two DISTINCT sets of valid `cqlite_*` Prometheus identifiers a dashboard
/// `expr` may reference (roborev #2427 r3, F3 — kept separate so an attribute
/// label never counts as a metric reference).
struct ValidNames {
    /// Exact sanitized METRIC-name series (from `operator_metric_docs()` —
    /// authoritative name+kind+unit). Membership here (and ONLY here) satisfies
    /// "the dashboard references a real metric".
    metrics: HashSet<String>,
    /// Sanitized bounded ATTRIBUTE keys used as PromQL label names / `by(...)`
    /// grouping (e.g. `cqlite_rpc_method`). Valid tokens, but referencing one
    /// does NOT count as referencing a metric.
    attributes: HashSet<String>,
}

impl ValidNames {
    /// A token is a valid `cqlite_*` identifier if it is either an exact metric
    /// series name or a sanitized attribute label.
    fn is_valid(&self, tok: &str) -> bool {
        self.metrics.contains(tok) || self.attributes.contains(tok)
    }
}

/// Build the metric and attribute identifier sets, both derived entirely from the
/// catalog (anti-drift). Metric names are the EXACT emitted series per
/// `sanitized_variants`; attributes are the sanitized bounded `catalog::attr::*`
/// keys.
fn valid_sanitized_names() -> ValidNames {
    let mut metrics = HashSet::new();
    let docs = operator_metric_docs()
        .expect("operator_metric_docs must succeed (every ALL_METRICS entry is annotated)");
    for d in &docs {
        for v in sanitized_variants(d.name, d.kind, d.unit) {
            metrics.insert(v);
        }
    }
    let attributes = ATTRIBUTE_KEYS.iter().map(|a| a.replace('.', "_")).collect();
    ValidNames {
        metrics,
        attributes,
    }
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

    // A dotted `cqlite.*` token is accepted iff it is an EXACT metric name, an
    // EXACT bounded attribute key, or an explicit `.*` wildcard group ref (a row
    // title such as `cqlite.flight.admission.*`). A bare namespace prefix is NOT
    // accepted (roborev #2427 r3, F2). Anything else is a renamed/removed/phantom
    // reference — fail CLOSED.
    let mut phantom: Vec<String> = referenced
        .iter()
        .filter(|name| {
            !catalog.contains(name.as_str())
                && !attrs.contains(name.as_str())
                && !is_wildcard_group_ref(name)
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
    // `referenced_any_metric` is set ONLY by membership in the METRIC set — an
    // attribute label alone does not count as referencing a metric (roborev #2427
    // r3, F3).
    let mut invalid: Vec<String> = Vec::new();
    let mut referenced_any_metric = false;
    for expr in &exprs {
        for tok in cqlite_expr_tokens(expr) {
            if valid.metrics.contains(&tok) {
                referenced_any_metric = true;
            } else if !valid.attributes.contains(&tok) {
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

    // The correct sanitized name IS in the metric set…
    assert!(
        valid
            .metrics
            .contains("cqlite_rpc_phase_duration_seconds_bucket"),
        "the correct histogram bucket name must be a valid sanitized variant"
    );
    // …but a one-character typo ("duraton") is NOT — so the panel would be empty.
    let typo_expr = "histogram_quantile(0.95, sum(rate(cqlite_rpc_phase_duraton_seconds_bucket\
                     {cluster=~\"$cluster\"}[5m])) by (le, cqlite_rpc_phase))";
    let mut invalid: Vec<String> = cqlite_expr_tokens(typo_expr)
        .into_iter()
        .filter(|t| !valid.is_valid(t))
        .collect();
    invalid.sort();
    assert_eq!(
        invalid,
        vec!["cqlite_rpc_phase_duraton_seconds_bucket".to_string()],
        "the expr validator must flag exactly the typo'd metric name (and nothing else)"
    );

    // Sanity: a counter's `_total` and a histogram's `_bucket`/`_count`/`_sum` are
    // all accepted (forward-derived from kind), a bare gauge name is accepted, and
    // a sanitized attribute label is accepted (in the ATTRIBUTE set, not metrics).
    assert!(
        valid.metrics.contains("cqlite_rpc_requests_total"),
        "counter _total"
    );
    assert!(
        valid.metrics.contains("cqlite_rpc_duration_seconds_count"),
        "histogram _count"
    );
    assert!(
        valid.metrics.contains("cqlite_rpc_duration_seconds_sum"),
        "histogram _sum"
    );
    assert!(
        valid.metrics.contains("cqlite_rpc_in_flight"),
        "bare gauge name"
    );
    assert!(
        valid.metrics.contains("cqlite_rpc_bytes_total"),
        "counter with By unit + _total"
    );
    assert!(
        valid.attributes.contains("cqlite_rpc_method"),
        "sanitized attribute label lives in the attribute set"
    );
    assert!(
        !valid.metrics.contains("cqlite_rpc_method"),
        "an attribute label is NOT a metric name (roborev #2427 r3, F3)"
    );
    // A name that already ends in `_total` is not double-suffixed.
    assert!(
        valid.metrics.contains("cqlite_errors_total"),
        "errors.total counter"
    );
    assert!(
        !valid.metrics.contains("cqlite_errors_total_total"),
        "must not double-append _total to a name already ending in _total"
    );

    // F1 (roborev #2427 r3) — the EXACT-name tightening: bare and mis-suffixed
    // forms that would render an EMPTY panel are REJECTED, not accepted.
    assert!(
        !valid.metrics.contains("cqlite_rpc_requests"),
        "bare counter name (missing _total) must be REJECTED — the collector emits \
         only cqlite_rpc_requests_total"
    );
    assert!(
        !valid.metrics.contains("cqlite_rpc_phase_duration_bucket"),
        "histogram bucket missing the _seconds unit stem must be REJECTED — the \
         collector emits cqlite_rpc_phase_duration_seconds_bucket"
    );
    assert!(
        !valid.metrics.contains("cqlite_rpc_duration"),
        "bare seconds-histogram name (no _seconds_{{bucket,count,sum}}) must be REJECTED"
    );
    // And they are flagged as invalid by the expr validator.
    let bare_counter_expr = "sum(rate(cqlite_rpc_requests{cluster=~\"$cluster\"}[5m]))";
    let mis_suffixed_hist_expr =
        "histogram_quantile(0.95, sum(rate(cqlite_rpc_phase_duration_bucket[5m])) by (le))";
    for (expr, want) in [
        (bare_counter_expr, "cqlite_rpc_requests"),
        (mis_suffixed_hist_expr, "cqlite_rpc_phase_duration_bucket"),
    ] {
        let flagged: Vec<String> = cqlite_expr_tokens(expr)
            .into_iter()
            .filter(|t| !valid.is_valid(t))
            .collect();
        assert!(
            flagged.contains(&want.to_string()),
            "expr validator must flag the bare/mis-suffixed form `{want}` (would render empty), \
             flagged: {flagged:?}"
        );
    }

    // F2 (roborev #2427 r3) — an explicit `.*` wildcard group ref is recognized,
    // but a bare namespace prefix is NOT.
    assert!(
        is_wildcard_group_ref("cqlite.flight.admission.*"),
        "explicit .* group ref covering real metrics must be accepted"
    );
    assert!(
        !is_wildcard_group_ref("cqlite.flight.admission"),
        "a bare namespace prefix (no trailing .*) must be REJECTED (roborev #2427 r3, F2)"
    );
    assert!(
        !is_wildcard_group_ref("cqlite.does.not.exist.*"),
        "a .* group ref covering NO real metric must be REJECTED"
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
