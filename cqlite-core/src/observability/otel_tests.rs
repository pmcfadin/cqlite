//! Tests for the OpenTelemetry exporter/runtime wiring, split out of `otel.rs` to
//! keep that file inside the campsite-rule source target (#1116).

use super::*;
use crate::observability::otel_instruments::build_instruments;
use opentelemetry::metrics::MeterProvider as _;
use opentelemetry_sdk::metrics::SdkMeterProvider;

#[test]
fn init_disabled_returns_inert_guard() {
    let cfg = ObservabilityConfig::builder().enabled(false).build();
    let guard = init(cfg).expect("inert init never fails");
    assert!(!guard.is_active());
    guard.force_flush(); // no-op, must not panic
}

#[test]
fn sampler_builds_for_ratio() {
    // Just exercise the builder for coverage; sampler has no public getter.
    let _ = build_sampler(0.5);
    let _ = build_sampler(2.0); // clamps internally
}

#[test]
fn traceparent_none_empty_and_invalid_are_noops() {
    // Must not panic for absent / blank / malformed headers.
    let span = tracing::info_span!("test");
    set_span_parent_from_traceparent(&span, None);
    set_span_parent_from_traceparent(&span, Some("   "));
    set_span_parent_from_traceparent(&span, Some("not-a-traceparent"));
}

#[test]
fn traceparent_valid_header_is_accepted() {
    // A well-formed W3C traceparent should parse and re-parent without panic.
    let span = tracing::info_span!("test");
    let valid = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    set_span_parent_from_traceparent(&span, Some(valid));
}

// ---------------------------------------------------------------------------
// Registration completeness, measured at RUNTIME (issue #1705, roborev B2).
//
// These call the SAME resolvers the emit path calls (`counter_for` /
// `histogram_for` / `gauge_for` in `otel.rs`), so "registered" means *this name
// resolves to a live, pre-built instrument*, not *this name is mentioned in the
// otel source*. A comment, a dead `let _ = catalog::X;`, or a deleted
// registration cannot satisfy them. They resolve against an ISOLATED instrument
// set built by the production `build_instruments` from a meter the test owns, so
// they measure the real registration table without touching the process-global
// `INSTRUMENTS`/`METER` `OnceLock`s — see `IsolatedInstruments` below.
//
// WHAT RESOLUTION ALONE CANNOT SEE, and why it no longer matters (#1705, F3):
// `Some(instrument)` proves an instrument exists for a name, never that it is the
// RIGHT one — a mis-wired dispatch arm (`catalog::READ_ROWS => &i.read_bytes`)
// resolved happily while emitting under the wrong series. That is fixed
// STRUCTURALLY rather than detected: instruments are keyed by the catalog name
// they were CONSTRUCTED with (`otel_instruments::Registry`), so lookup cannot name
// a different metric than construction did, and `catalog_tests.rs` reds on any
// hand-written per-metric arm that would reintroduce the second statement.
//
// SCOPE, stated honestly: this file only compiles under
// `--features observability` (the whole `otel` module is gated), so the default
// gate run does NOT execute these. The always-compiled companion is the
// structural registration parse in `catalog_tests.rs`, which is deliberately
// narrowed to the `Registry` registration calls for that reason.
// ---------------------------------------------------------------------------

/// An instrument set built from a meter this test OWNS (issue #1705).
///
/// # These guards must never call `instruments()`
///
/// `otel_instruments::instruments()` is a process-wide `OnceLock`, built from
/// `otel::meter()` — itself a `OnceLock` over `global::meter(SCOPE)`. Both are
/// one-shot, so whichever test touches them FIRST in a binary decides what the
/// global meter is for the rest of the process. These guards install no meter
/// provider (they have nothing to export and no reason to), so calling
/// `instruments()` from here would bind the global meter to the NO-OP provider
/// whenever this file happened to run first — and every later
/// `testing::metrics_capture()` test in the same binary would then observe no
/// metrics at all. That is an order-dependent failure, i.e. the worst kind.
///
/// Running the capture first would only ORDER around the hazard. Building against a
/// locally-owned [`SdkMeterProvider`] REMOVES it: nothing here reads or writes any
/// `OnceLock`, in any order, and the file needs no `observability-testing` feature.
///
/// What is measured is unchanged. `build_instruments` runs the same three
/// `register_*` passes the production set is built from, so the registration table
/// read here is exactly the one the emit path resolves through `counter_for` /
/// `histogram_for` / `gauge_for` — the very functions called below.
struct IsolatedInstruments {
    /// Owns the metric pipeline the instruments were built against; held for the
    /// lifetime of the guard so nothing observes a torn-down provider.
    _provider: SdkMeterProvider,
    instruments: Instruments,
}

impl IsolatedInstruments {
    fn build() -> Self {
        let provider = SdkMeterProvider::builder().build();
        let meter = provider.meter(SCOPE);
        let instruments = build_instruments(&meter);
        Self {
            _provider: provider,
            instruments,
        }
    }

    /// How many of the three instrument kinds resolve `name` to a live instrument.
    fn resolved_kinds(&self, name: &str) -> usize {
        let i = &self.instruments;
        [
            counter_for(i, name).is_some(),
            histogram_for(i, name).is_some(),
            gauge_for(i, name).is_some(),
        ]
        .into_iter()
        .filter(|resolved| *resolved)
        .count()
    }
}

#[test]
fn every_catalogued_metric_resolves_to_exactly_one_live_instrument_or_is_stats_only() {
    let iso = IsolatedInstruments::build();
    let stats_only: std::collections::HashSet<&str> =
        catalog::STATS_ONLY_METRICS.iter().map(|m| m.name).collect();
    let mut unresolved = Vec::new();
    let mut ambiguous = Vec::new();
    let mut stats_only_but_wired = Vec::new();

    for name in catalog::ALL_METRICS {
        let kinds = iso.resolved_kinds(name);
        if stats_only.contains(name) {
            if kinds != 0 {
                stats_only_but_wired.push(*name);
            }
            continue;
        }
        match kinds {
            0 => unresolved.push(*name),
            1 => {}
            _ => ambiguous.push(*name),
        }
    }

    assert!(
        unresolved.is_empty(),
        "catalogued metrics resolve to NO live instrument (wire them in \
         otel_instruments.rs + the matching resolver, or declare them in \
         catalog::STATS_ONLY_METRICS with their reason): {unresolved:?}"
    );
    assert!(
        ambiguous.is_empty(),
        "catalogued metrics resolve to more than one instrument kind: {ambiguous:?}"
    );
    assert!(
        stats_only_but_wired.is_empty(),
        "metrics declared STATS_ONLY have a live instrument — delete the stale \
         exemption: {stats_only_but_wired:?}"
    );
}

#[test]
fn an_unregistered_name_resolves_to_no_instrument() {
    let iso = IsolatedInstruments::build();
    // The negative half: resolution is an affirmative measurement, so a name that
    // is not registered must fail to resolve. Without this, a resolver that
    // returned `Some` for everything would make the guard above vacuous.
    for bogus in [
        "cqlite.definitely.not.a.metric",
        "",
        "cqlite.read.rows.but.longer",
        "read.rows",
    ] {
        assert_eq!(
            iso.resolved_kinds(bogus),
            0,
            "{bogus:?} must not resolve to any dedicated instrument"
        );
    }
    // And a name that IS registered resolves — proving the probe can say yes.
    assert_eq!(iso.resolved_kinds(catalog::READ_ROWS), 1);
    assert_eq!(iso.resolved_kinds(catalog::READ_DURATION), 1);
    assert_eq!(iso.resolved_kinds(catalog::SSTABLES_OPEN), 1);
}

#[test]
fn every_live_instrument_is_catalogued_and_resolves_only_as_its_own_kind() {
    // The FORWARD direction at RUNTIME (#1705, F3): walk the instrument set that was
    // actually built and assert every key is catalogued. The structural parse in
    // `catalog_tests.rs` asserts the same thing about the SOURCE; this asserts it
    // about the live objects, so a registration the parser somehow misreads is still
    // caught here.
    let iso = IsolatedInstruments::build();
    let i = &iso.instruments;
    let catalogued: std::collections::HashSet<&str> =
        catalog::ALL_METRICS.iter().copied().collect();
    let live: Vec<(&str, &str)> = i
        .counters
        .keys()
        .map(|n| ("counter", *n))
        .chain(i.histograms.keys().map(|n| ("histogram", *n)))
        .chain(i.gauges.keys().map(|n| ("gauge", *n)))
        .collect();

    let uncatalogued: Vec<&(&str, &str)> = live
        .iter()
        .filter(|(_, name)| !catalogued.contains(name))
        .collect();
    assert!(
        uncatalogued.is_empty(),
        "live instruments exist for metrics ABSENT from ALL_METRICS: {uncatalogued:?}"
    );

    // Each key resolves as exactly ONE kind, so no name is registered twice across
    // kinds (which would make `add_counter` and `record_gauge` disagree about it).
    for (kind, name) in &live {
        assert_eq!(
            iso.resolved_kinds(name),
            1,
            "{name} (registered as a {kind}) must resolve as exactly one kind"
        );
    }

    // And the affirmative COUNT: every catalogued metric except the declared
    // stats-only ones has a live instrument, so the sets are equal — not merely
    // "no surprises found", which an empty instrument set would also satisfy.
    // Counted by filtering rather than subtracting, so the expectation stands on
    // its own instead of assuming STATS_ONLY_METRICS ⊆ ALL_METRICS (asserted
    // elsewhere) and underflowing if it ever does not.
    let stats_only: std::collections::HashSet<&str> =
        catalog::STATS_ONLY_METRICS.iter().map(|m| m.name).collect();
    let expected = catalog::ALL_METRICS
        .iter()
        .filter(|name| !stats_only.contains(*name))
        .count();
    assert_eq!(
        live.len(),
        expected,
        "the live instrument set must be exactly the catalogued metrics that are not \
         declared STATS_ONLY"
    );
}
