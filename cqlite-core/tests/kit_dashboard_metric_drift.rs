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
/// contains at least one real catalog metric BELOW it — e.g.
/// `cqlite.flight.admission.*` (covers `cqlite.flight.admission.in_use`, …). ONLY
/// the explicit-wildcard form is admitted as a group ref (roborev #2427 r3, F2): a
/// BARE dotted prefix such as `cqlite.flight.admission` (no trailing `.*`) is NOT a
/// valid reference, so a phantom that merely happens to be a prefix of a real
/// namespace can no longer pass.
///
/// A `<stem>.*` is valid ONLY if ≥1 catalog metric name starts with the literal
/// `"<stem>."` — i.e. a real CHILD lives under the namespace (roborev #2427 r4, F1).
/// A wildcard on an exact LEAF metric with no children — `cqlite.errors.total.*`
/// where `cqlite.errors.total` IS a metric but nothing lives below it — covers
/// nothing and is REJECTED; the earlier `*m == stem` clause fail-open here let such
/// a leaf-wildcard pass. `cqlite.does.not.exist.*` (no such namespace at all) also
/// remains rejected.
fn is_wildcard_group_ref(token: &str) -> bool {
    let Some(stem) = token.strip_suffix(".*") else {
        return false;
    };
    let dotted = format!("{stem}.");
    ALL_METRICS.iter().any(|m| m.starts_with(&dotted))
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

/// PromQL grouping / vector-matching keywords whose FOLLOWING `( … )` holds LABEL
/// keys, not metric selectors (`sum(...) by (le, cqlite_rpc_method)`). Identifiers
/// inside such a paren group — and these keywords themselves — are never metric
/// (vector-selector) position tokens.
const PROMQL_GROUPING_KEYWORDS: &[&str] = &[
    "by",
    "without",
    "on",
    "ignoring",
    "group_left",
    "group_right",
];

/// Bare PromQL words that can appear at brace-depth 0 in identifier position but
/// are NOT series selectors — logical/set binary operators and misc keywords that
/// stand without an immediately-following `(`.
const PROMQL_RESERVED_WORDS: &[&str] = &[
    "and", "or", "unless", "bool", "offset", "atan2", "inf", "nan", "start", "end",
];

/// Extract every identifier in METRIC (instant-vector-selector) POSITION from a
/// PromQL `expr`, REGARDLESS of prefix (roborev #2427 r4, F2). Unlike
/// [`cqlite_expr_tokens`] — which only sees `cqlite_`-prefixed tokens and so cannot
/// catch a *prefix* typo like `cqltie_rpc_requests_total` — this classifies by
/// syntactic position. The dashboard is cqlite-only, so EVERY metric-position token
/// must be a recognized catalog series; anything else renders an EMPTY panel and
/// must FAIL. Excludes, by construction:
///   - function/aggregation names (identifier immediately followed by `(`),
///   - label keys inside `{ … }` selectors and quoted strings,
///   - identifiers inside `[ … ]` range/duration brackets,
///   - grouping-clause label lists (`by`/`without`/`on`/`ignoring`/`group_*`),
///   - bare reserved binary keywords.
fn metric_position_tokens(expr: &str) -> Vec<String> {
    let mut out = Vec::new();
    let bytes = expr.as_bytes();
    let mut i = 0;
    let mut brace_depth = 0usize; // inside `{ … }` label selectors
    let mut bracket_depth = 0usize; // inside `[ … ]` range / duration
                                    // Paren stack: true == this `( … )` is a grouping label list.
    let mut paren_is_label_list: Vec<bool> = Vec::new();
    let mut next_paren_is_label_list = false;
    while i < bytes.len() {
        let c = bytes[i];
        match c {
            b'"' | b'\'' => {
                // Skip a quoted string literal wholesale (handle `\` escapes).
                let quote = c;
                i += 1;
                while i < bytes.len() && bytes[i] != quote {
                    if bytes[i] == b'\\' {
                        i += 1;
                    }
                    i += 1;
                }
                i += 1; // consume closing quote (or run off end)
            }
            b'{' => {
                brace_depth += 1;
                i += 1;
            }
            b'}' => {
                brace_depth = brace_depth.saturating_sub(1);
                i += 1;
            }
            b'[' => {
                bracket_depth += 1;
                i += 1;
            }
            b']' => {
                bracket_depth = bracket_depth.saturating_sub(1);
                i += 1;
            }
            b'(' => {
                paren_is_label_list.push(next_paren_is_label_list);
                next_paren_is_label_list = false;
                i += 1;
            }
            b')' => {
                paren_is_label_list.pop();
                i += 1;
            }
            _ if c.is_ascii_alphabetic() || c == b'_' => {
                let start = i;
                while i < bytes.len()
                    && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_' || bytes[i] == b':')
                {
                    i += 1;
                }
                let tok = &expr[start..i];
                // Peek the next non-space char to detect a function/keyword call.
                let mut j = i;
                while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                    j += 1;
                }
                let followed_by_paren = j < bytes.len() && bytes[j] == b'(';
                if followed_by_paren {
                    // Function or grouping keyword — never a metric itself. A
                    // grouping keyword marks its following paren as a label list.
                    if PROMQL_GROUPING_KEYWORDS.contains(&tok) {
                        next_paren_is_label_list = true;
                    }
                    continue;
                }
                let in_label_context = brace_depth > 0
                    || bracket_depth > 0
                    || paren_is_label_list.last().copied().unwrap_or(false);
                let is_reserved =
                    PROMQL_GROUPING_KEYWORDS.contains(&tok) || PROMQL_RESERVED_WORDS.contains(&tok);
                if !in_label_context && !is_reserved {
                    out.push(tok.to_string());
                }
            }
            _ => {
                i += 1;
            }
        }
    }
    out
}

/// Classify one expr's metric-position tokens against the catalog, returning
/// `(referenced_a_catalog_metric, offending_tokens)` (roborev #2427 r4, F2). A
/// metric-position identifier that isn't a known catalog series is an offender EVEN
/// IF it isn't `cqlite_`-prefixed — which is exactly how a *prefix* typo
/// (`cqltie_rpc_requests_total`) is caught. Sanitized attribute labels are accepted
/// but do NOT count as referencing a metric (roborev #2427 r3, F3).
fn classify_expr(expr: &str, valid: &ValidNames) -> (bool, Vec<String>) {
    let mut referenced = false;
    let mut offenders = Vec::new();
    for tok in metric_position_tokens(expr) {
        if valid.metrics.contains(&tok) {
            referenced = true;
        } else if !valid.attributes.contains(&tok) {
            offenders.push(tok);
        }
    }
    (referenced, offenders)
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

    // Validate PER EXPR (roborev #2427 r4, F2), by metric POSITION rather than by
    // `cqlite_` prefix, so that (a) a *prefix* typo like `cqltie_rpc_requests_total`
    // is caught (the old `cqlite_`-only scanner ignored it), and (b) EVERY panel
    // target must ITSELF reference ≥1 recognized catalog metric — a valid sibling
    // panel no longer masks a typo'd one that renders EMPTY. Both conditions fail
    // CLOSED, naming the offending token and its expr.
    let mut invalid: Vec<String> = Vec::new();
    let mut exprs_without_metric: Vec<String> = Vec::new();
    for expr in &exprs {
        let (referenced_metric, offenders) = classify_expr(expr, &valid);
        for tok in offenders {
            invalid.push(format!("{tok}  (in expr: {expr})"));
        }
        if !referenced_metric {
            exprs_without_metric.push(expr.clone());
        }
    }
    invalid.sort();
    invalid.dedup();
    assert!(
        invalid.is_empty(),
        "kit dashboard {DASHBOARD_REL} has expr metric-position name(s) that are NOT a valid \
         sanitized catalog metric (a typo/rename — incl. a mistyped PREFIX — renders the panel \
         EMPTY; fix the expr or the catalog): {invalid:#?}"
    );
    assert!(
        exprs_without_metric.is_empty(),
        "kit dashboard {DASHBOARD_REL} has panel target expr(s) that reference NO recognized \
         catalog metric (would render EMPTY): {exprs_without_metric:#?}"
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

    // F1 (roborev #2427 r4) — a wildcard on an exact LEAF metric with no children
    // must be REJECTED: `cqlite.errors.total` IS a metric, but no catalog series
    // starts with `cqlite.errors.total.`, so `cqlite.errors.total.*` covers nothing
    // and would be a dead group ref. The removed `*m == stem` clause used to let it
    // pass (fail-open).
    assert!(
        ALL_METRICS.contains(&"cqlite.errors.total"),
        "precondition: cqlite.errors.total is an exact catalog leaf metric"
    );
    assert!(
        !ALL_METRICS
            .iter()
            .any(|m| m.starts_with("cqlite.errors.total.")),
        "precondition: no catalog metric lives BELOW cqlite.errors.total"
    );
    assert!(
        !is_wildcard_group_ref("cqlite.errors.total.*"),
        "a wildcard on an exact leaf metric with no children must be REJECTED \
         (roborev #2427 r4, F1) — the removed `*m == stem` clause let it pass"
    );
    // The dashboard's real wildcard row titles DO have children, so they still pass.
    assert!(
        is_wildcard_group_ref("cqlite.warm.cache.*"),
        "a real namespace with children must still be accepted"
    );
}

#[test]
fn per_expr_prefix_typo_fails_even_with_a_valid_sibling_panel_negative_test() {
    // FINDING 2 negative test (roborev #2427 r4): metric coverage is checked PER
    // EXPR, not globally, and by metric POSITION (not `cqlite_` prefix) — so a panel
    // whose ONLY selector is a *prefix*-typo'd metric (`cqltie_rpc_requests_total`)
    // FAILS even when OTHER panels are valid. The old global+prefix-scoped check
    // passed this: the typo has the wrong prefix (invisible to a `cqlite_`-only
    // scanner) and a sibling panel satisfied the single global coverage flag.
    let valid = valid_sanitized_names();

    // A valid panel and a prefix-typo'd panel, exactly as they would coexist.
    let valid_expr = "sum(rate(cqlite_rpc_requests_total{cluster=~\"$cluster\"}[1m])) \
                      by (cqlite_rpc_method)";
    let typo_expr = "sum(rate(cqltie_rpc_requests_total{cluster=~\"$cluster\"}[1m]))";

    // The valid panel references a real metric; the typo panel references NONE.
    let (valid_ref, valid_off) = classify_expr(valid_expr, &valid);
    assert!(valid_ref, "the valid sibling expr references a real metric");
    assert!(
        valid_off.is_empty(),
        "the valid sibling expr has no offending tokens, got: {valid_off:?}"
    );

    let (typo_ref, typo_off) = classify_expr(typo_expr, &valid);
    assert!(
        !typo_ref,
        "the prefix-typo'd expr references NO recognized catalog metric — the \
         per-expr coverage check must FAIL it even beside a valid sibling"
    );
    assert_eq!(
        typo_off,
        vec!["cqltie_rpc_requests_total".to_string()],
        "the prefix typo must be flagged as an offending metric-position token \
         (a `cqlite_`-only scanner would have missed the mistyped prefix)"
    );

    // `metric_position_tokens` isolates the SELECTOR position: function names,
    // grouping-clause label keys, and `{…}` matcher keys are NOT metric-position.
    let hist_expr = "histogram_quantile(0.95, sum(rate(\
                     cqlite_rpc_duration_seconds_bucket{cluster=~\"$cluster\"}[5m])) \
                     by (le, cqlite_rpc_method))";
    let positions = metric_position_tokens(hist_expr);
    assert_eq!(
        positions,
        vec!["cqlite_rpc_duration_seconds_bucket".to_string()],
        "only the vector-selector metric is in metric position; \
         histogram_quantile/sum/rate (functions), le, cqlite_rpc_method (grouping \
         labels) and cluster ({{}} matcher key) are excluded, got: {positions:?}"
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
