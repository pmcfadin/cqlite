//! Tests for the OpenTelemetry exporter/runtime wiring, split out of `otel.rs` to
//! keep that file inside the campsite-rule source target (#1116).

use super::*;

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
