//! Issue #1702 (epic #1686, AI2 "observability honesty"): asking for OTel on a
//! build that compiled observability OUT must be VISIBLE, not silent.
//!
//! # The defect this pins
//!
//! `observability` is NOT a default feature, so the shipped default build links
//! no OTel stack at all. `CQLITE_OTEL_ENABLED=1` still parses cleanly into
//! `ObservabilityConfig { enabled: true, .. }` and `observability::init` still
//! returns an inert guard — so metrics and traces are dropped on the floor with
//! no signal anywhere. From the operator's chair "collector down /
//! misconfigured endpoint" and "the binary was built without the feature" look
//! IDENTICAL. The fix is visibility (ONE warning at init), never an error: a
//! degraded-but-running process is the correct behavior.
//!
//! # Why `tracing`, not `log`
//!
//! `cqlite-core` has NO `log` dependency; its facade is `tracing` (the #1706
//! log->tracing migration). `tracing::warn!` satisfies the same constraint the
//! issue text spells as `log::warn!`: the CLI's fmt layer writes to STDERR
//! only, so stdout stays clean for `--out json/csv` (issue #129).
//!
//! # Scope of this file
//!
//! Feature-OFF only (`#![cfg(not(feature = "observability"))]`). AC2 ("a
//! feature-enabled build emits no such warning") is STRUCTURAL, not behavioral:
//! the emit exists only inside the `cfg(not(feature = "observability"))` copy of
//! `init`, so in a feature-on build it is not compiled at all — there is no
//! runtime path to test, and a feature-on assertion would be vacuous. The CLI
//! end-to-end half (warning on stderr, stdout unaffected) lives in
//! `cqlite-cli/tests/issue_1702_cli_otel_feature_off_warning.rs`.

#![cfg(not(feature = "observability"))]

use std::sync::{Arc, Mutex};

use cqlite_core::observability::{init, ObservabilityConfig};
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::prelude::*;

/// One captured `tracing` event: its level and its formatted `message` field.
#[derive(Clone, Debug)]
struct Captured {
    level: Level,
    message: String,
}

/// Thread-safe capture sink shared with the layer.
#[derive(Clone, Default)]
struct Sink(Arc<Mutex<Vec<Captured>>>);

impl Sink {
    fn events(&self) -> Vec<Captured> {
        self.0
            .lock()
            .expect("capture sink mutex is never poisoned in this test")
            .clone()
    }

    fn warnings(&self) -> Vec<Captured> {
        self.events()
            .into_iter()
            .filter(|e| e.level == Level::WARN)
            .collect()
    }
}

/// Records the `message` field of an event into a string.
#[derive(Default)]
struct MessageVisitor(String);

impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        }
    }
}

impl<S: Subscriber> Layer<S> for Sink {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);
        self.0
            .lock()
            .expect("capture sink mutex is never poisoned in this test")
            .push(Captured {
                level: *event.metadata().level(),
                message: visitor.0,
            });
    }
}

/// Run `f` with a capturing subscriber installed on THIS thread and return the
/// events it emitted. Thread-local (`with_default`) is sufficient and correct:
/// `init` is synchronous and spawns nothing, so every event it emits is on the
/// calling thread. Thread-local also keeps concurrent tests in this binary from
/// observing each other's events.
fn capture<T>(f: impl FnOnce() -> T) -> (T, Sink) {
    let sink = Sink::default();
    let subscriber = tracing_subscriber::registry().with(sink.clone());
    let out = tracing::subscriber::with_default(subscriber, f);
    (out, sink)
}

/// AC1 (the red one): a default-features build asked for OTel emits exactly ONE
/// WARN that names the env var, the missing cargo feature, and the consequence.
#[test]
fn enabled_config_warns_once_naming_feature_and_consequence() {
    let cfg = ObservabilityConfig::builder().enabled(true).build();
    let (guard, sink) = capture(|| init(cfg).expect("init never fails with the feature off"));
    assert!(
        !guard.is_active(),
        "the feature-off guard is inert regardless of the warning"
    );

    let warnings = sink.warnings();
    assert_eq!(
        warnings.len(),
        1,
        "expected EXACTLY ONE warning at init (not zero — the #1702 silent \
         no-op — and not one per operation); captured: {:?}",
        sink.events()
    );

    let msg = &warnings[0].message;
    let lower = msg.to_lowercase();
    // Names the knob the operator set...
    assert!(
        msg.contains("CQLITE_OTEL_ENABLED"),
        "warning must name the env var so the operator can tie it to what they set: {msg}"
    );
    // ...the cargo feature that is missing (this is what distinguishes "built
    // without the feature" from "collector down")...
    assert!(
        msg.contains("observability"),
        "warning must name the `observability` cargo feature: {msg}"
    );
    // ...and the consequence, in operator terms.
    assert!(
        lower.contains("metric") && lower.contains("trace"),
        "warning must say metrics and traces are affected: {msg}"
    );
    assert!(
        lower.contains("no metrics") && lower.contains("will be emitted"),
        "warning must state that nothing will be emitted: {msg}"
    );
}

/// The negative: the default (OTel not requested) build must stay silent. A
/// warning here would fire on every process start of every default build.
#[test]
fn disabled_config_emits_no_warning() {
    let cfg = ObservabilityConfig::builder().enabled(false).build();
    let (_guard, sink) = capture(|| init(cfg).expect("init never fails with the feature off"));
    assert!(
        sink.warnings().is_empty(),
        "a config that never asked for OTel must not warn; captured: {:?}",
        sink.events()
    );
}

/// Sibling-toggle consistency (issue #1702 item 3). `verify_presence_oracle` is
/// the only other `ObservabilityConfig` field a feature-off build acts on (the
/// remaining six — endpoint, protocol, service_name, service_version,
/// sampling_ratio, timeout — are subordinate to `enabled` and mean nothing once
/// it is off), and it IS honored with the feature off — `init` plumbs it into the always-compiled presence-verification
/// switch — so it is NOT silently dropped and must NOT warn. `enabled` is the
/// only input a feature-off build discards.
#[test]
fn presence_oracle_toggle_alone_does_not_warn() {
    let cfg = ObservabilityConfig::builder()
        .enabled(false)
        .verify_presence_oracle(true)
        .build();
    let (_guard, sink) = capture(|| init(cfg).expect("init never fails with the feature off"));
    assert!(
        sink.warnings().is_empty(),
        "verify_presence_oracle is honored in a feature-off build, so enabling \
         it must not warn; captured: {:?}",
        sink.events()
    );

    // No cleanup of the process-global presence-verification switch this `init`
    // flipped: no test in this binary consults it (directly or through a
    // reader), so there is nothing here for it to leak into. A reset would also
    // be worse than nothing — it pins the switch OFF rather than restoring
    // UNINIT, it is unordered against the sibling tests cargo runs on parallel
    // threads, and it would be skipped entirely if an assertion above panicked.
    // A future test in this file that DOES read the switch must serialize
    // instead (e.g. `serial_test`), not rely on a trailing reset.
}
