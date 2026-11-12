//! Unit tests for `export.rs` (moved out for the #1116 file-size ratchet; module path unchanged).

use super::*;

// Spec R5 (Total-resolution and ETA-eligibility are independently
// verifiable): a present `--limit` resolves to a determinate total and is
// ETA-eligible.
#[test]
fn limit_present_resolves_to_determinate_total() {
    let resolved = resolve_progress_total(Some(42));
    assert_eq!(resolved.total, Some(42));
    assert!(
        resolved.eta_eligible,
        "a known total must be ETA-eligible (determinate bar)"
    );
}

// Spec R5: an absent `--limit` resolves to an unknown total (spinner) and
// is not ETA-eligible.
#[test]
fn limit_absent_resolves_to_unknown_total() {
    let resolved = resolve_progress_total(None);
    assert_eq!(resolved.total, None);
    assert!(
        !resolved.eta_eligible,
        "an unknown total must not be ETA-eligible (spinner, no ETA)"
    );
}

// Spec R1/R2/R5: the determinate-vs-spinner choice is driven by the
// resolved total's ETA-eligibility (a pure, terminal-free function), so the
// `eta_eligible` flag is the load-bearing signal — not a second derivation
// from `total`.
#[test]
fn total_selects_bar_kind() {
    assert_eq!(
        progress_bar_kind(resolve_progress_total(Some(100))),
        BarKind::Determinate
    );
    assert_eq!(
        progress_bar_kind(resolve_progress_total(None)),
        BarKind::Spinner
    );
}

// Spec R1 (determinate bar promises ETA + percent, observable without a
// TTY): the determinate template carries `{eta}` and `{percent}` plus the
// `{bar}`/`{pos}`/`{len}` evidence of a real bar.
#[test]
fn determinate_template_contains_eta_and_percent() {
    let t = progress_template(progress_bar_kind(resolve_progress_total(Some(42))));
    assert!(t.contains("{eta}"), "must show an ETA: {t:?}");
    assert!(t.contains("{percent}"), "must show a percent: {t:?}");
    assert!(t.contains("{bar"), "must render a bar: {t:?}");
    assert!(
        t.contains("{pos}") && t.contains("{len}"),
        "must show pos/len: {t:?}"
    );
}

// Spec R2 (spinner promises no ETA / no percent, observable without a TTY):
// the spinner template carries `{spinner}` and a `{pos}` row count but
// never `{eta}` or `{percent}`.
#[test]
fn spinner_template_omits_eta_and_percent() {
    let t = progress_template(progress_bar_kind(resolve_progress_total(None)));
    assert!(t.contains("{spinner"), "must render a spinner: {t:?}");
    assert!(t.contains("{pos}"), "must show a row count: {t:?}");
    assert!(!t.contains("{eta}"), "must NOT show an ETA: {t:?}");
    assert!(!t.contains("{percent}"), "must NOT show a percent: {t:?}");
}
