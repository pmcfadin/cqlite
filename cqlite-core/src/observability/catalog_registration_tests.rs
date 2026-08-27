//! Instrument-REGISTRATION guards for the metric catalog, split out of
//! `catalog_tests.rs` to keep both files inside the campsite-rule test target
//! (#1135). A nested `mod` of that file, so it reads the same helpers
//! (`otel_sources_uncommented`, `strip_rust_comments`, `value_to_ident`, …) rather
//! than re-implementing them — CLAUDE.md: "a port is a second implementation".
//!
//! Everything here answers one question in two directions: does the set of metric
//! names with a LIVE OTel instrument equal the set `catalog::ALL_METRICS`
//! advertises (minus the declared `STATS_ONLY_METRICS`)? The parsers read the otel
//! sources structurally, so the checks run on the DEFAULT gate build, where the
//! `observability` feature — and therefore the runtime resolution guard in
//! `otel_tests.rs` — is not compiled at all.

use super::*;

/// The OTel builder methods that construct an instrument.
const INSTRUMENT_BUILDERS: [&str; 6] = [
    ".u64_counter(",
    ".f64_counter(",
    ".u64_histogram(",
    ".f64_histogram(",
    ".i64_gauge(",
    ".u64_gauge(",
];

/// The `otel_instruments::Registry` methods that REGISTER a catalog metric, and
/// the instrument kind each produces.
///
/// A call to one of these is the WHOLE registration (issue #1705, roborev F3): the
/// method uses its `name` parameter both as the OTel instrument name and as the
/// map key the resolver looks up, so construction and dispatch are one construct
/// and cannot name different metrics. That is why these calls — not a builder call
/// plus a match arm — are what the guards below read.
const REGISTRATION_CALLS: [(&str, &str); 3] = [
    (".counter(", "counter"),
    (".histogram(", "histogram"),
    (".gauge(", "gauge"),
];

/// The three name→instrument resolvers in `otel.rs` (`add_counter` /
/// `record_histogram` / `record_gauge` each call one). They are keyed map lookups
/// with NO per-metric code; [`handwritten_dispatch_arms`] exists to keep them that
/// way.
const OTEL_RESOLVERS: [&str; 3] = ["fn counter_for", "fn histogram_for", "fn gauge_for"];

/// The leading `catalog::IDENT` of `arg`, or `None` if `arg` does not open with one.
///
/// rustfmt may wrap between a call's `(` and its first argument, so callers pass
/// already-`trim_start`ed text.
fn leading_catalog_ident(arg: &str) -> Option<String> {
    let rest = arg.strip_prefix("catalog::")?;
    let ident: String = rest
        .chars()
        .take_while(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || *c == '_')
        .collect();
    (!ident.is_empty()).then_some(ident)
}

/// The first argument text of the call whose `(` ends at `src[..end]`, truncated to
/// something short enough to name in an error message.
fn call_argument(src: &str, end: usize) -> String {
    src[end..]
        .trim_start()
        .chars()
        .take(40)
        .collect::<String>()
        .replace('\n', " ")
}

/// Every catalog metric REGISTERED in the otel sources → the instrument kind it is
/// registered as, or `Err` on a registration this parser cannot account for.
///
/// **Fail-closed on an unrecognised argument shape (issue #1705, roborev F4).** The
/// previous extractor SKIPPED any recognised builder call whose argument was not
/// literally `catalog::IDENT`, so an instrument registered with a string literal
/// (`reg.counter("cqlite.ghost", …)`) or a local alias (`reg.counter(GHOST, …)`) was
/// invisible to `every_instrument_registered_in_otel_is_catalogued` — an
/// uncatalogued instrument passing a guard written to catch exactly that. Skipping
/// the unrecognised case is the permissive-branch-for-an-unmeasured-input shape
/// CLAUDE.md forbids, so an argument this parser cannot classify is now an ERROR.
fn parse_registrations(src: &str) -> Result<std::collections::BTreeMap<String, String>, String> {
    let mut out = std::collections::BTreeMap::new();
    for (call, kind) in REGISTRATION_CALLS {
        for (i, _) in src.match_indices(call) {
            let end = i + call.len();
            let arg = src[end..].trim_start();
            let Some(ident) = leading_catalog_ident(arg) else {
                return Err(format!(
                    "`{call}` is a Registry registration whose first argument is not a \
                     `catalog::IDENT` constant: {:?}. Register metrics by their catalog \
                     constant — a string literal or a local alias hides the metric from \
                     the catalogue guards (#1705, F4)",
                    call_argument(src, end)
                ));
            };
            if let Some(prior) = out.insert(ident.clone(), kind.to_string()) {
                if prior != kind {
                    return Err(format!(
                        "catalog::{ident} is registered as both a {prior} and a {kind}"
                    ));
                }
                return Err(format!(
                    "catalog::{ident} is registered twice as a {kind} — the second \
                     registration silently replaces the first"
                ));
            }
        }
    }
    if out.is_empty() {
        return Err(
            "no Registry registrations found — REGISTRATION_CALLS no longer matches the \
             otel sources, and a guard with an empty subject set passes vacuously"
                .to_string(),
        );
    }
    Ok(out)
}

/// Catalog metrics built DIRECTLY by an instrument-builder call (rather than through
/// the Registry), or `Err` on a builder call this parser cannot account for.
///
/// Today the set is empty: `Registry` owns every construction. The check still runs
/// because it is what fails closed (issue #1705, roborev F4) if someone hand-builds
/// an instrument again — with a catalog constant (recorded here, so the catalogue
/// guards see it) or with anything else (an error).
///
/// **The exempt calls, and why each is exempt.** Both shapes pass `name`, a
/// parameter, never a metric name written at the call site:
///
/// * the three `Registry` helpers (`self.meter.u64_counter(name)` …) — `name` IS the
///   registered catalog name, bound in the same call by
///   [`parse_registrations`] above;
/// * the three ad-hoc fallbacks in `otel.rs` (`meter().u64_counter(name)` in
///   `add_counter` / `record_histogram` / `record_gauge`) — these build an instrument
///   for a caller-supplied NON-catalog name so a call site never silently drops
///   data. They are deliberately not registrations and must not be catalogued.
fn parse_builder_constructions(src: &str) -> Result<std::collections::BTreeSet<String>, String> {
    let mut out = std::collections::BTreeSet::new();
    let mut registry_helpers = 0usize;
    let mut adhoc_fallbacks = 0usize;
    for builder in INSTRUMENT_BUILDERS {
        for (i, _) in src.match_indices(builder) {
            let end = i + builder.len();
            let arg = src[end..].trim_start();
            if let Some(ident) = leading_catalog_ident(arg) {
                out.insert(ident);
                continue;
            }
            // `name`, and only `name`, may be a parameter — attributed to one of the
            // two exempt shapes by its receiver.
            let is_name_param = arg
                .strip_prefix("name")
                .is_some_and(|rest| rest.trim_start().starts_with(')'));
            // The receiver tells the two exempt shapes apart. rustfmt may put the
            // chain on its own lines (`self\n    .meter\n    .u64_counter(name)`), so
            // whitespace is collapsed before matching — and the two markers are
            // disjoint: a field access reads `.meter`, the free function `meter()`.
            let receiver: String = src[i.saturating_sub(64)..i]
                .split_whitespace()
                .collect::<Vec<_>>()
                .join("");
            if is_name_param && receiver.ends_with("meter()") {
                adhoc_fallbacks += 1;
                continue;
            }
            if is_name_param && receiver.ends_with(".meter") {
                registry_helpers += 1;
                continue;
            }
            return Err(format!(
                "`{builder}` is called with an argument this guard cannot classify: {:?}. \
                 An instrument must be built either from a `catalog::IDENT` constant or \
                 by a `Registry` helper / the ad-hoc `_ =>` fallback from its `name` \
                 parameter — anything else (a string literal, a local alias) would hide \
                 the metric from the catalogue guards (#1705, F4)",
                call_argument(src, end)
            ));
        }
    }
    if registry_helpers != 3 {
        return Err(format!(
            "expected the 3 `Registry` instrument helpers, found {registry_helpers} — \
             the construction site moved, so this guard is no longer reading it"
        ));
    }
    if adhoc_fallbacks != 3 {
        return Err(format!(
            "expected the 3 ad-hoc `_ =>` fallbacks in otel.rs, found {adhoc_fallbacks} — \
             either a fallback was added without being classified here, or the emit path \
             no longer has one"
        ));
    }
    Ok(out)
}

/// The bodies of the three [`OTEL_RESOLVERS`], or `Err` if one is missing.
///
/// Fail-closed on a missing resolver: renaming or deleting one must red the guards,
/// not silently empty them (a guard whose subject set shrinks to nothing passes
/// vacuously, which is the defect class these fixes exist to remove).
fn resolver_bodies(src: &str) -> Result<Vec<&str>, String> {
    let mut out = Vec::new();
    for resolver in OTEL_RESOLVERS {
        let start = src.find(resolver).ok_or_else(|| {
            format!(
                "`{resolver}` not found in the otel sources — the emit path resolves \
                 names through these three functions, so renaming one must be reflected \
                 in OTEL_RESOLVERS rather than leaving the guards blind"
            )
        })?;
        let body = &src[start..];
        let end = body
            .find("\n}\n")
            .ok_or_else(|| format!("`{resolver}` must end at a column-0 closing brace"))?;
        out.push(&body[..end]);
    }
    Ok(out)
}

/// Catalog constants appearing as a hand-written MATCH ARM inside a resolver.
///
/// Must always be EMPTY (issue #1705, roborev F3). A per-metric arm restates a name
/// the construction already stated, and two statements of one fact can disagree:
/// `catalog::READ_ROWS => &i.read_bytes` routes emissions to the wrong series while
/// resolving to a live instrument, so neither the structural parse nor the runtime
/// resolution can see it — they only ever ask whether SOME instrument exists for the
/// name. The instruments are keyed by their construction name instead, so an arm
/// like that is now UNREPRESENTABLE — unless someone hand-writes a dispatch table
/// again, which is what this rejects. A correctly-wired arm is rejected too: the
/// construct is what is unsafe, and a correct one is one edit away from a mis-wire
/// with nothing able to tell the difference.
fn handwritten_dispatch_arms(src: &str) -> Result<std::collections::BTreeSet<String>, String> {
    let mut out = std::collections::BTreeSet::new();
    for body in resolver_bodies(src)? {
        for (i, _) in body.match_indices("catalog::") {
            let rest = &body[i..];
            let Some(ident) = leading_catalog_ident(rest) else {
                continue;
            };
            // Only an arm PATTERN counts: `catalog::IDENT =>` (rustfmt may wrap
            // before the `=>`). A mention anywhere else in the body does not.
            if rest["catalog::".len() + ident.len()..]
                .trim_start()
                .starts_with("=>")
            {
                out.insert(ident);
            }
        }
    }
    Ok(out)
}

/// Assert the resolvers carry no per-metric dispatch — see
/// [`handwritten_dispatch_arms`].
fn assert_resolvers_have_no_handwritten_dispatch(src: &str) {
    match handwritten_dispatch_arms(src) {
        Err(why) => panic!("the otel resolvers could not be parsed: {why}"),
        Ok(arms) if !arms.is_empty() => panic!(
            "the otel resolvers contain hand-written per-metric dispatch arms for \
             {arms:?}. Registration must stay ONE construct (a `Registry` call), or a \
             name/instrument mismatch becomes representable again and invisible to every \
             guard (#1705, F3)"
        ),
        Ok(_) => {}
    }
}

/// Catalog metric → the instrument kind it is registered as. Panics with the
/// parser's own diagnosis on anything it cannot account for.
fn otel_registrations(src: &str) -> std::collections::BTreeMap<String, String> {
    parse_registrations(src).unwrap_or_else(|why| panic!("otel registration parse failed: {why}"))
}

/// Names with SOME instrument binding — registered through the `Registry`, or built
/// directly by a builder call, or (illegally) routed by a hand-written arm.
///
/// Used by the FORWARD guard ("an instrument exists whose name is not catalogued"),
/// where the union is the fail-closed choice: a half-wired instrument still must be
/// catalogued.
fn otel_instrument_bindings(src: &str) -> std::collections::BTreeSet<String> {
    let mut out: std::collections::BTreeSet<String> = otel_registrations(src).into_keys().collect();
    out.extend(
        parse_builder_constructions(src)
            .unwrap_or_else(|why| panic!("otel construction parse failed: {why}")),
    );
    out.extend(
        handwritten_dispatch_arms(src)
            .unwrap_or_else(|why| panic!("otel resolver parse failed: {why}")),
    );
    out
}

/// Names AFFIRMATIVELY registered as a live instrument.
///
/// Used by the REVERSE guard ("a catalogued name no instrument is bound to"). This
/// used to be the INTERSECTION of a construction set and a dispatch set, because
/// half a wiring is not a scrapeable series. Registration is now a single construct
/// that does both (issue #1705, F3), so there are no halves left to intersect —
/// a `Registry` call is the affirmative evidence, and a builder call without one is
/// not (its instrument is never reachable by name from the emit path).
///
/// This is the always-compiled counterpart of the RUNTIME resolution asserted in
/// `otel_tests.rs` (which calls the very resolvers the emit path calls, but only
/// compiles under `--features observability`). Both must agree; the structural parse
/// exists so the default gate run is not blind.
fn otel_registered_instruments(src: &str) -> std::collections::BTreeSet<String> {
    assert_resolvers_have_no_handwritten_dispatch(src);
    otel_registrations(src).into_keys().collect()
}

#[test]
fn partition_access_probe_metrics_have_dedicated_registrations_not_the_adhoc_fallback() {
    // Issue #2827: without a dedicated registration these fall through
    // `add_counter`'s ad-hoc `_ =>` branch, which builds a fresh instrument per emit
    // and exports the series with NO unit (`By`, `{partition}`) and no description —
    // and, by construction, `every_instrument_registered_in_otel_is_catalogued`
    // cannot see them either. Assert the registrations exist at the source level,
    // like the #2419 saturation-gauge guard below.
    //
    // Registration is now ONE construct per metric (#1705, F3), so this asserts the
    // registration AND the KIND it is registered as — a counter registered as a
    // gauge would exports the wrong instrument type, which the old
    // "a `catalog::X =>` arm exists somewhere" text search could not tell apart.
    assert_every_otel_source_is_scanned();
    let registrations = otel_registrations(&otel_sources_uncommented());
    for (metric, ident, kind) in [
        (
            READ_PARTITION_ACCESS_DISTINCT_PARTITIONS,
            "READ_PARTITION_ACCESS_DISTINCT_PARTITIONS",
            "counter",
        ),
        (
            READ_PARTITION_ACCESS_ACCESSES,
            "READ_PARTITION_ACCESS_ACCESSES",
            "counter",
        ),
        (
            READ_PARTITION_ACCESS_BYTES,
            "READ_PARTITION_ACCESS_BYTES",
            "counter",
        ),
        (
            READ_PARTITION_ACCESS_SAMPLE_DENOMINATOR,
            "READ_PARTITION_ACCESS_SAMPLE_DENOMINATOR",
            "gauge",
        ),
    ] {
        assert_eq!(
            registrations.get(ident).map(String::as_str),
            Some(kind),
            "{metric} must be registered as a dedicated {kind} so its series carries \
             its catalogued unit and description, never the ad-hoc fallback"
        );
    }
    // And with their catalogued units, which the registration call states as the
    // argument right after the name (whitespace-insensitive: rustfmt decides where
    // the call wraps).
    let otel_src = otel_sources_uncommented();
    for (ident, unit) in [
        ("READ_PARTITION_ACCESS_BYTES", "BYTES"),
        ("READ_PARTITION_ACCESS_SAMPLE_DENOMINATOR", "DIMENSIONLESS"),
    ] {
        let at = otel_src
            .find(&format!("catalog::{ident},"))
            .unwrap_or_else(|| panic!("no registration call for catalog::{ident}"));
        let tail: String = otel_src[at..].chars().take(160).collect();
        assert!(
            tail.contains(&format!("catalog::unit::{unit}")),
            "catalog::{ident} must be registered with unit::{unit}: {tail:?}"
        );
    }
}

#[test]
fn every_instrument_registered_in_otel_is_catalogued() {
    // Issue #2426 (roborev MEDIUM, F1): guard the "emitted instrument absent
    // from ALL_METRICS" bug class. The otel sources are the canonical instrument
    // registration + record-routing site (every cross-crate emission — incl.
    // cqlite-flight's warm-cache/admission metrics — routes through
    // `add_counter`/`record_histogram`/`record_gauge`, which resolve the
    // registered instruments). Any `catalog::SCREAMING_CONST` BOUND to an
    // instrument there is a metric name with a live series, so it MUST appear in
    // `ALL_METRICS`. This is a fully-automatic source-level check (no
    // `observability` feature needed): register an instrument and forget to
    // catalogue it → this fails.
    //
    // Automation note (#2426): because every catalogued instrument that
    // cqlite-flight emits is registered here (never the ad-hoc `_ =>` fallback),
    // the check transitively covers the flight emission sites too. A future metric
    // emitted ONLY via the ad-hoc fallback (no registration, no catalog entry)
    // would not be caught here — that path is reserved for genuinely non-catalog
    // names. BOTH otel sources are scanned (#1116 split): `otel_instruments.rs`
    // holds the registrations, `otel.rs` the resolvers and fallbacks, and reading
    // one alone would let a binding in the other escape the guard.
    assert_every_otel_source_is_scanned();
    let otel_src = otel_sources_uncommented();
    let otel_src = otel_src.as_str();
    let catalogued: std::collections::HashSet<&str> = ALL_METRICS.iter().copied().collect();

    // Collect the const IDENTIFIERS present in the ALL_METRICS array so we can
    // map an `otel.rs` `catalog::IDENT` reference to a catalogued name. The
    // constants are `pub const IDENT: &str = "cqlite. …";`.
    //
    // Parsed over the WHOLE declaration, not line-by-line. rustfmt wraps a long
    // declaration onto two lines:
    //
    // ```ignore
    // pub const READ_PARTITION_ACCESS_DISTINCT_PARTITIONS: &str =
    //     "cqlite.read.partition_access.distinct_partitions";
    // ```
    //
    // A line-scoped parser finds no string literal on the `pub const` line and
    // drops the constant from the map — so the very metrics most likely to be new
    // (long names) would slip past this guard, which is the bug class it exists to
    // catch. Scan from each `pub const ` to its terminating `;` instead.
    let this_src = include_str!("catalog.rs");
    let ident_to_value = parse_str_consts(this_src);

    // Names with SOME instrument binding — constructed, routed, or both. The UNION
    // is the fail-closed choice HERE (#1705, roborev B2): a half-wired instrument
    // still must be catalogued. Only real construction/dispatch constructs count;
    // a comment or a dead reference is not an instrument.
    let mut missing = Vec::new();
    let mut refs: Vec<String> = otel_instrument_bindings(otel_src).into_iter().collect();
    refs.sort();
    for ident in refs {
        let value = ident_to_value.get(ident.as_str()).copied().unwrap_or_else(|| {
            panic!("otel.rs binds an instrument to catalog::{ident}, which is not a metric-name constant in catalog.rs")
        });
        if !catalogued.contains(value) {
            missing.push(format!("catalog::{ident} (\"{value}\")"));
        }
    }
    assert!(
        missing.is_empty(),
        "otel.rs registers instruments for metrics ABSENT from ALL_METRICS \
         (add them to catalog::ALL_METRICS): {missing:?}"
    );
}

#[test]
fn every_catalogued_metric_is_otel_registered_or_declared_stats_only() {
    // Issue #1705 (AI5) — the REVERSE of
    // `every_instrument_registered_in_otel_is_catalogued`, and the half that was
    // missing: a PHANTOM catalog entry, i.e. a name in `ALL_METRICS` that no
    // instrument is ever bound to. `operator_docs` generates the operator-facing
    // metrics reference from `ALL_METRICS`, so a phantom entry advertises a series
    // an operator can never scrape — the observability-honesty failure epic #1686
    // exists to close.
    //
    // A name is accounted for in exactly one of two ways: an instrument is
    // AFFIRMATIVELY registered for it — a `Registry` registration call, which binds
    // the name to the instrument in ONE construct ([`otel_registered_instruments`])
    // — or it is DECLARED in `catalog::STATS_ONLY_METRICS`. Nothing else passes.
    //
    // Strictness note (#1705, roborev B2/F4): this used to accept any textual
    // `catalog::CONST` occurrence in the otel sources, so removing a registration
    // while leaving a comment or a dead reference behind kept the guard green. Only
    // the registration calls are authoritative, comments are stripped first, and a
    // registration whose argument this guard cannot classify is an ERROR rather than
    // a skipped line.
    assert_every_otel_source_is_scanned();
    let refs = otel_registered_instruments(&otel_sources_uncommented());
    let value_to_ident = value_to_ident();
    let stats_only: std::collections::HashSet<&str> =
        STATS_ONLY_METRICS.iter().map(|m| m.name).collect();

    let mut phantom = Vec::new();
    for name in ALL_METRICS {
        let ident = value_to_ident.get(name).copied().unwrap_or_else(|| {
            panic!("ALL_METRICS entry {name:?} has no `pub const` declaration in catalog.rs")
        });
        if !refs.contains(ident) && !stats_only.contains(name) {
            phantom.push(format!("catalog::{ident} (\"{name}\")"));
        }
    }
    assert!(
        phantom.is_empty(),
        "ALL_METRICS names metrics with NO registered otel instrument and no \
         STATS_ONLY_METRICS declaration — either wire the instrument in \
         otel_instruments.rs/otel.rs, or declare it stats-only with its reason: \
         {phantom:?}"
    );
}

#[test]
fn stats_only_metrics_are_catalogued_and_never_otel_registered() {
    // Issue #1705: keep the exemption list from rotting in the OTHER direction.
    // Once an instrument IS wired for a name, its exemption must be deleted — a
    // stale entry would permanently excuse that name from the reverse guard.
    assert_every_otel_source_is_scanned();
    // UNION here: a stats-only metric must have NO instrument binding at all, not
    // merely an incomplete one.
    let refs = otel_instrument_bindings(&otel_sources_uncommented());
    let value_to_ident = value_to_ident();
    let mut seen = std::collections::HashSet::new();
    for name in STATS_ONLY_METRICS.iter().map(|m| m.name) {
        assert!(
            ALL_METRICS.contains(&name),
            "{name} is declared stats-only but is not in ALL_METRICS"
        );
        assert!(
            seen.insert(name),
            "duplicate STATS_ONLY_METRICS entry {name}"
        );
        let ident = value_to_ident.get(name).copied().unwrap_or_else(|| {
            panic!("STATS_ONLY_METRICS entry {name:?} has no `pub const` in catalog.rs")
        });
        assert!(
            !refs.contains(ident),
            "catalog::{ident} (\"{name}\") IS registered as an otel instrument — \
             remove it from STATS_ONLY_METRICS, or the reverse registration guard \
             carries a stale exemption"
        );
    }
}

#[test]
fn saturation_gauges_have_dedicated_registrations_not_the_adhoc_fallback() {
    // Issue #2419 (WS2), spec Requirement / #2412 lesson: each saturation gauge must
    // resolve to a pre-built instrument, NOT the ad-hoc `_ =>` fallback (which
    // rebuilds the instrument on every sample). Source-scan the otel sources for a
    // dedicated GAUGE registration per gauge — a fully-automatic check needing no
    // `observability` feature. Delete a registration → this fails. Scans BOTH otel
    // sources (#1116 split).
    assert_every_otel_source_is_scanned();
    let registrations = otel_registrations(&otel_sources_uncommented());
    for ident in [
        "MERGE_EGRESS_CHANNEL_DEPTH",
        "MERGE_ACTIVE_MERGES",
        "PROC_THREADS",
        "PROC_FDS",
        "PROC_RSS_BYTES",
        "FLIGHT_BLOCKING_TASKS_IN_USE",
        "FLIGHT_TABLES_DISCOVERED",
        "FLIGHT_WARM_TABLES",
    ] {
        assert_eq!(
            registrations.get(ident).map(String::as_str),
            Some("gauge"),
            "catalog::{ident} has no dedicated GAUGE registration — the gauge would \
             fall through to the ad-hoc per-call-rebuilt fallback (#2412)"
        );
    }
}

/// A synthetic otel source shaped like the real one — three keyed-lookup resolvers,
/// the three `Registry` helpers, the three ad-hoc fallbacks — so the registration
/// parsers can be exercised on text this test controls. The parsers under test are
/// the ones the guards call, never a copy of them (CLAUDE.md: "a port is a second
/// implementation").
fn synthetic_otel_source(resolver_extra: &str, registrations: &str) -> String {
    format!(
        "fn counter_for(i: &Instruments, name: &str) -> Option<&Counter<u64>> {{\n\
         {resolver_extra}    i.counters.get(name)\n}}\n\
         fn histogram_for() {{\n    i.histograms.get(name)\n}}\n\
         fn gauge_for() {{\n    i.gauges.get(name)\n}}\n\
         fn add_counter() {{\n    meter().u64_counter(name).build()\n}}\n\
         fn record_histogram() {{\n    meter().f64_histogram(name).build()\n}}\n\
         fn record_gauge() {{\n    meter().i64_gauge(name).build()\n}}\n\
         impl Registry {{\n\
         \x20   fn counter(&mut self) {{ self.meter.u64_counter(name).build() }}\n\
         \x20   fn histogram(&mut self) {{ self.meter.f64_histogram(name).build() }}\n\
         \x20   fn gauge(&mut self) {{ self.meter.i64_gauge(name).build() }}\n}}\n\
         fn register_all(reg: &mut Registry) {{\n\
         \x20   reg.counter(catalog::ANCHOR, catalog::unit::ROWS, \"anchor\");\n\
         {registrations}}}\n"
    )
}

/// Parse a synthetic source the way the guards do: comments stripped first.
/// Returns `(has SOME binding for GHOST, is GHOST affirmatively registered)`.
fn synthetic_registered(resolver_extra: &str, registrations: &str) -> (bool, bool) {
    let src = strip_rust_comments(&synthetic_otel_source(resolver_extra, registrations));
    (
        otel_instrument_bindings(&src).contains("GHOST"),
        otel_registered_instruments(&src).contains("GHOST"),
    )
}

#[test]
fn a_comment_or_a_dead_reference_cannot_pass_as_a_registered_instrument() {
    // Issue #1705 (roborev B2) — the defect this replaces: the old extractor took
    // ANY textual `catalog::CONST` occurrence as proof that an instrument existed,
    // so deleting a registration while leaving a mention behind kept the reverse
    // guard green. Prove that neither shape registers anything now.
    let commented_out = synthetic_registered(
        "",
        "    // reg.counter(catalog::GHOST, catalog::unit::ROWS, \"g\");\n",
    );
    assert_eq!(
        commented_out,
        (false, false),
        "a commented-out registration must register nothing"
    );

    let doc_link = synthetic_registered("", "    /* reg.counter(catalog::GHOST, u, \"g\") */\n");
    assert_eq!(
        doc_link,
        (false, false),
        "a doc link / block comment must register nothing"
    );

    let dead_code = synthetic_registered("", "    let _ = catalog::GHOST;\n");
    assert_eq!(
        dead_code,
        (false, false),
        "a dead reference that registers no instrument must register nothing"
    );
}

#[test]
fn a_registry_call_is_the_whole_registration() {
    // Registration used to be TWO constructs (a builder call + a dispatch arm) and
    // the reverse guard intersected them, because half a wiring is not a scrapeable
    // series. It is now ONE construct (#1705, F3), so a single `Registry` call is
    // both a binding and an affirmative registration — and there is no way to write
    // half of it.
    assert_eq!(
        synthetic_registered(
            "",
            "    reg.counter(catalog::GHOST, catalog::unit::ROWS, \"g\");\n"
        ),
        (true, true),
        "a Registry call registers the metric outright"
    );
    // rustfmt wraps long calls; the wrapped shape must still be seen — wrapping
    // selects for LONG names, i.e. exactly the new metrics these guards exist to
    // catch.
    assert_eq!(
        synthetic_registered(
            "",
            "    reg.gauge(\n        catalog::GHOST,\n        catalog::unit::ROWS,\n        \"g\",\n    );\n"
        ),
        (true, true),
        "a wrapped registration must not drop out"
    );
}

#[test]
fn a_handwritten_dispatch_arm_is_rejected_because_it_can_mis_wire() {
    // Issue #1705 (roborev F3) — the RED test for the finding. A per-metric arm
    // restates a name the construction already stated, and the two can disagree:
    // `catalog::READ_ROWS => &i.read_bytes` resolves to a LIVE instrument, so the
    // runtime completeness guard says "registered" and the structural parse says
    // "routed", while every emission lands under `cqlite.read.bytes`. Nothing we had
    // could see it. Keyed lookup makes it unrepresentable; this guard is what keeps
    // it that way.
    let mis_wired = strip_rust_comments(&synthetic_otel_source(
        "    let _ = match name { catalog::READ_ROWS => &i.read_bytes, _ => return None };\n",
        "",
    ));
    let arms = handwritten_dispatch_arms(&mis_wired).expect("the resolvers must parse");
    assert!(
        arms.contains("READ_ROWS"),
        "a mis-wired dispatch arm must be detected: {arms:?}"
    );
    let outcome = catch_panic(|| assert_resolvers_have_no_handwritten_dispatch(&mis_wired));
    assert!(
        outcome.is_err(),
        "a mis-wired dispatch arm must RED the guard, not merely be listed"
    );

    // A CORRECTLY-wired arm is rejected too: the construct is what is unsafe. A
    // reviewer cannot tell a right arm from a wrong one without checking every
    // field by hand, which is the check that kept passing while wrong.
    let hand_wired = strip_rust_comments(&synthetic_otel_source(
        "    let _ = match name { catalog::READ_ROWS => &i.read_rows, _ => return None };\n",
        "",
    ));
    assert!(
        catch_panic(|| assert_resolvers_have_no_handwritten_dispatch(&hand_wired)).is_err(),
        "any per-metric dispatch arm must be rejected, correct-looking or not"
    );

    // And the REAL sources must be clean — otherwise the guard above is asserting a
    // property the shipped code does not have.
    assert_every_otel_source_is_scanned();
    assert_resolvers_have_no_handwritten_dispatch(&otel_sources_uncommented());
}

#[test]
fn an_unrecognised_registration_argument_fails_closed() {
    // Issue #1705 (roborev F4) — the RED test for the finding. The old extractor
    // SKIPPED a recognised builder call whose argument was not literally
    // `catalog::IDENT`, so an instrument registered with a string literal or a local
    // alias was invisible to `every_instrument_registered_in_otel_is_catalogued`:
    // an uncatalogued instrument passing the guard written to catch it. An
    // unmeasured input must never take the permissive branch.
    for (label, registration) in [
        (
            "string literal",
            "    reg.counter(\"cqlite.ghost\", catalog::unit::ROWS, \"g\");\n",
        ),
        (
            "local alias",
            "    reg.counter(GHOST_NAME, catalog::unit::ROWS, \"g\");\n",
        ),
        (
            "function call",
            "    reg.gauge(ghost_name(), catalog::unit::ROWS, \"g\");\n",
        ),
    ] {
        let src = strip_rust_comments(&synthetic_otel_source("", registration));
        let why = parse_registrations(&src)
            .expect_err(&format!("a {label} registration argument must fail closed"));
        assert!(
            why.contains("not a\n                     `catalog::IDENT` constant")
                || why.contains("`catalog::IDENT` constant"),
            "unexpected rejection reason for a {label}: {why}"
        );
    }

    // The same rule on the BUILDER calls: a hand-built instrument named by a literal
    // or an alias is rejected; only a `catalog::IDENT` (recorded) or the six exempt
    // `name`-parameter calls (the three Registry helpers + the three ad-hoc
    // fallbacks) pass.
    let literal_builder = strip_rust_comments(&synthetic_otel_source(
        "    let _ = meter().u64_counter(\"cqlite.ghost\").build();\n",
        "",
    ));
    let why = parse_builder_constructions(&literal_builder)
        .expect_err("a string-literal builder argument must fail closed");
    assert!(
        why.contains("cannot classify"),
        "unexpected rejection reason: {why}"
    );
    let alias_builder = strip_rust_comments(&synthetic_otel_source(
        "    let _ = meter().i64_gauge(GHOST_NAME).build();\n",
        "",
    ));
    assert!(parse_builder_constructions(&alias_builder).is_err());

    // A hand-built instrument named by a catalog constant is not an error — it is a
    // BINDING the forward guard must see, so it is recorded rather than skipped.
    let catalog_builder = strip_rust_comments(&synthetic_otel_source(
        "    let _ = meter().u64_counter(catalog::GHOST).build();\n",
        "",
    ));
    assert!(parse_builder_constructions(&catalog_builder)
        .expect("a catalog-named builder call is classifiable")
        .contains("GHOST"));

    // And the affirmative half: the synthetic source's exempt calls parse cleanly,
    // so the rejections above come from the argument shape and not from the harness.
    let clean = strip_rust_comments(&synthetic_otel_source("", ""));
    assert_eq!(
        parse_builder_constructions(&clean).expect("the exempt calls must classify"),
        std::collections::BTreeSet::new()
    );
    // As must the REAL sources — the six exempt calls are counted, so adding a
    // seventh construction site reds this rather than slipping past.
    assert_every_otel_source_is_scanned();
    assert!(parse_builder_constructions(&otel_sources_uncommented())
        .expect("the real otel sources must classify")
        .is_empty());
}

/// Run `f`, returning `Err` if it panicked, with the panic hook silenced so a
/// deliberately-failing guard does not spew a backtrace into the test log.
fn catch_panic<F: FnOnce() + std::panic::UnwindSafe>(f: F) -> Result<(), ()> {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let outcome = std::panic::catch_unwind(f);
    std::panic::set_hook(previous);
    outcome.map_err(|_| ())
}

#[test]
fn a_missing_resolver_reds_the_parse_instead_of_emptying_it() {
    // A guard whose subject set silently shrinks to nothing passes vacuously, so
    // renaming/removing a resolver must fail the parse rather than yield an empty
    // arm set.
    let no_gauge_resolver =
        "fn counter_for() {\n    i.counters.get(name)\n}\nfn histogram_for() {\n    i.histograms.get(name)\n}\n";
    let why = resolver_bodies(no_gauge_resolver)
        .expect_err("a missing resolver must fail the parse, not quietly return nothing");
    assert!(why.contains("fn gauge_for"), "unexpected reason: {why}");
    assert!(handwritten_dispatch_arms(no_gauge_resolver).is_err());
    assert!(
        catch_panic(|| assert_resolvers_have_no_handwritten_dispatch(no_gauge_resolver)).is_err(),
        "the assertion wrapper must red on an unparseable resolver set"
    );

    // And an empty registration set is an error for the same reason.
    let no_registrations = "fn counter_for() {}\n";
    assert!(parse_registrations(no_registrations).is_err());
}

#[test]
fn the_real_otel_sources_register_wired_metrics_and_not_the_stats_only_ones() {
    // The affirmative half on the REAL source: the parsers must actually say YES
    // for wired metrics (one per instrument kind) and NO for the declared
    // stats-only ones — otherwise the two guards above could both be vacuous.
    assert_every_otel_source_is_scanned();
    let registered = otel_registered_instruments(&otel_sources_uncommented());
    for wired in ["READ_ROWS", "READ_DURATION", "SSTABLES_OPEN"] {
        assert!(
            registered.contains(wired),
            "catalog::{wired} is wired in the otel sources but the parser did not \
             see it registered"
        );
    }
    for stats_only in ["KEY_CACHE_HITS", "KEY_CACHE_CAPACITY_BYTES"] {
        assert!(
            !registered.contains(stats_only),
            "catalog::{stats_only} is declared stats-only, so nothing may register it"
        );
    }
    assert!(
        registered.len() > 60,
        "the registered set collapsed to {} entries — the parse is broken, and a \
         shrunken subject set is a vacuous guard",
        registered.len()
    );
}
