//! `--max-concurrent-scans` precedence and provenance (issue #3225, §4, AC4).
//!
//! Precedence, highest first: **flag → `CQLITE_MAX_CONCURRENT_SCANS` →
//! parallelism-derived default**. An explicit value is honoured AS GIVEN and is
//! never clamped toward the derived one; only #2420's `[1,
//! Semaphore::MAX_PERMITS]` clamp still applies. The startup event reports the
//! effective (post-clamp) ceiling, which of the four provenance values produced
//! it, and the `P` it was derived from.
//!
//! Every precedence case here is driven through the **REAL clap parser** —
//! `cqlite_flight::cli::Args`, `env =` attributes and all — because a
//! hand-constructed config would prove nothing about what an operator's command
//! line does. That is why the CLI surface lives in the library
//! (`cqlite-flight/src/cli.rs`) rather than in `main.rs`.
//!
//! The `derived-fallback` arm is driven by INJECTING the oracle's `Err` into the
//! pure resolver: `available_parallelism()` answers on every platform this suite
//! runs on, so the arm would otherwise be untestable — and an untested arm is
//! exactly where a "looks derived but wasn't" label would hide.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use clap::CommandFactory;
use cqlite_flight::admission::{
    self, derive_max_concurrent_scans, probe_available_parallelism, Admission, AdmissionConfig,
    ExplicitScansOrigin, MaxConcurrentScansSource, WaitBudget, DEFAULT_MAX_CONCURRENT_SCANS,
    ENV_MAX_CONCURRENT_SCANS,
};
use cqlite_flight::cli::{self, Args, ARG_MAX_CONCURRENT_SCANS};
use serial_test::serial;

/// The allocator name these tests hand the parser (issue #3997). This target is
/// an INTEGRATION test, so `main.rs` — and therefore the real
/// `#[global_allocator]` and its `ALLOCATOR` const — is not compiled into it;
/// nothing here asserts on the allocator, so a fixed placeholder is honest. The
/// allocator's own end-to-end assertion drives the BUILT BINARY, in
/// `issue_3997_allocator_surface.rs`.
const TEST_ALLOCATOR: &str = "system";

/// Parse a command line through the real parser and resolve the ceiling.
fn resolve(argv: &[&str]) -> admission::ResolvedMaxConcurrentScans {
    let mut full = vec!["cqlite-flight", "--data-dir", "/tmp/cqlite-flight-test"];
    full.extend_from_slice(argv);
    let (args, matches) =
        Args::try_parse_with_matches_from(TEST_ALLOCATOR, full).expect("argv must parse");
    cli::resolve_max_concurrent_scans(&args, &matches)
}

/// Run `body` with `CQLITE_MAX_CONCURRENT_SCANS` set (or removed), restoring the
/// previous state afterwards. The tests that use it are `#[serial]`.
fn with_env<R>(value: Option<&str>, body: impl FnOnce() -> R) -> R {
    let previous = std::env::var(ENV_MAX_CONCURRENT_SCANS).ok();
    match value {
        Some(v) => std::env::set_var(ENV_MAX_CONCURRENT_SCANS, v),
        None => std::env::remove_var(ENV_MAX_CONCURRENT_SCANS),
    }
    let outcome = body();
    match previous {
        Some(v) => std::env::set_var(ENV_MAX_CONCURRENT_SCANS, v),
        None => std::env::remove_var(ENV_MAX_CONCURRENT_SCANS),
    }
    outcome
}

/// This host's derived ceiling — the `D` the precedence cases must differ from.
fn derived_here() -> usize {
    derive_max_concurrent_scans(
        probe_available_parallelism().expect("available_parallelism() answers on this host"),
    )
}

/// An explicit value distinct from both the derived default and the env value,
/// and comfortably inside #2420's clamp, so a passing assertion cannot be a
/// coincidence.
fn distinct_explicit_values() -> (usize, usize) {
    // (flag, env) — both far from any derived value on any host in the
    // [2, 64] derived range.
    (513, 257)
}

#[test]
fn the_provenance_lookup_targets_an_argument_that_exists() {
    // Affirmative measurement: provenance is read by asking `ArgMatches` for
    // this exact id. A field rename would silently degrade every explicit value
    // to `derived`, so assert the id is real rather than trusting the string.
    let command = Args::command();
    assert!(
        command
            .get_arguments()
            .any(|a| a.get_id() == ARG_MAX_CONCURRENT_SCANS),
        "clap has no `{ARG_MAX_CONCURRENT_SCANS}` argument — provenance would always report derived"
    );
}

#[test]
fn the_flag_carries_no_default_value_so_typed_64_is_distinguishable() {
    // `default_value_t` cannot tell "the operator typed 64" from "nobody typed
    // anything", and that distinction IS AC4. Assert the default is gone.
    let command = Args::command();
    let arg = command
        .get_arguments()
        .find(|a| a.get_id() == ARG_MAX_CONCURRENT_SCANS)
        .expect("the argument exists");
    assert!(
        arg.get_default_values().is_empty(),
        "--max-concurrent-scans must carry no clap default; provenance depends on its absence"
    );
}

#[test]
#[serial]
fn the_flag_wins_over_both_the_environment_and_the_derived_value() {
    let (flag, env) = distinct_explicit_values();
    let derived = derived_here();
    assert_ne!(flag, derived);
    assert_ne!(env, derived);
    let resolved = with_env(Some(&env.to_string()), || {
        resolve(&["--max-concurrent-scans", &flag.to_string()])
    });
    assert_eq!(resolved.value, flag);
    assert_eq!(resolved.source, MaxConcurrentScansSource::Flag);
}

#[test]
#[serial]
fn the_environment_wins_over_the_derived_value() {
    let (_flag, env) = distinct_explicit_values();
    assert_ne!(env, derived_here());
    let resolved = with_env(Some(&env.to_string()), || resolve(&[]));
    assert_eq!(resolved.value, env);
    assert_eq!(resolved.source, MaxConcurrentScansSource::Env);
}

#[test]
#[serial]
fn nothing_configured_yields_the_derived_value_with_its_input() {
    let resolved = with_env(None, || resolve(&[]));
    let p = probe_available_parallelism().expect("available_parallelism() answers on this host");
    assert_eq!(resolved.value, derive_max_concurrent_scans(p));
    assert_eq!(resolved.source, MaxConcurrentScansSource::Derived);
    assert_eq!(resolved.available_parallelism, Some(p));
}

#[test]
fn an_unavailable_oracle_yields_the_previous_constant_labelled_distinctly() {
    // The injected-`Err` arm: no explicit config AND no parallelism reading.
    let resolved = admission::resolve_max_concurrent_scans(None, None);
    assert_eq!(resolved.value, DEFAULT_MAX_CONCURRENT_SCANS);
    assert_eq!(resolved.source, MaxConcurrentScansSource::DerivedFallback);
    assert_eq!(
        resolved.available_parallelism, None,
        "the field the startup log omits must be absent, not a placeholder"
    );
    assert_ne!(
        resolved.source,
        MaxConcurrentScansSource::Derived,
        "an unconsulted oracle must never be reported as a derived value"
    );
}

#[test]
#[serial]
fn an_explicit_value_above_the_derived_default_is_honoured_not_clamped_down() {
    // The derived default is a DEFAULT, not a cap: an explicit ceiling well
    // above it survives all the way to the semaphore.
    let explicit = 512usize;
    assert!(explicit > derived_here());
    let resolved = with_env(None, || {
        resolve(&["--max-concurrent-scans", &explicit.to_string()])
    });
    assert_eq!(resolved.value, explicit);
    assert_eq!(admission_limit_for(resolved.value), explicit);
}

#[test]
#[serial]
fn the_previous_default_is_restorable_with_one_setting_on_any_host() {
    // `--max-concurrent-scans 64` reproduces the pre-#3225 ceiling exactly,
    // whatever this host derives.
    let resolved = with_env(None, || {
        resolve(&[
            "--max-concurrent-scans",
            &DEFAULT_MAX_CONCURRENT_SCANS.to_string(),
        ])
    });
    assert_eq!(resolved.value, DEFAULT_MAX_CONCURRENT_SCANS);
    assert_eq!(resolved.source, MaxConcurrentScansSource::Flag);
    assert_eq!(
        admission_limit_for(resolved.value),
        DEFAULT_MAX_CONCURRENT_SCANS
    );
}

#[test]
#[serial]
fn a_configured_value_equal_to_the_derived_one_is_still_labelled_configured() {
    // Provenance is about the ROUTE, not the number: a coincidence must not
    // read as "derived".
    let derived = derived_here();
    let resolved = with_env(None, || {
        resolve(&["--max-concurrent-scans", &derived.to_string()])
    });
    assert_eq!(resolved.value, derived);
    assert_eq!(resolved.source, MaxConcurrentScansSource::Flag);

    let resolved = with_env(Some(&derived.to_string()), || resolve(&[]));
    assert_eq!(resolved.value, derived);
    assert_eq!(resolved.source, MaxConcurrentScansSource::Env);
}

#[test]
fn the_pure_resolver_labels_each_explicit_origin() {
    // The clap-independent half of the contract, so the mapping is pinned even
    // if the CLI surface changes shape.
    let flag = admission::resolve_max_concurrent_scans(
        Some((9, ExplicitScansOrigin::CommandLine)),
        Some(8),
    );
    assert_eq!(
        (flag.value, flag.source),
        (9, MaxConcurrentScansSource::Flag)
    );
    let env = admission::resolve_max_concurrent_scans(
        Some((9, ExplicitScansOrigin::Environment)),
        Some(8),
    );
    assert_eq!((env.value, env.source), (9, MaxConcurrentScansSource::Env));
}

#[test]
fn the_four_provenance_values_have_four_distinct_spellings() {
    let spellings = [
        MaxConcurrentScansSource::Flag.as_str(),
        MaxConcurrentScansSource::Env.as_str(),
        MaxConcurrentScansSource::Derived.as_str(),
        MaxConcurrentScansSource::DerivedFallback.as_str(),
    ];
    assert_eq!(spellings, ["flag", "env", "derived", "derived-fallback"]);
    let unique: std::collections::BTreeSet<_> = spellings.iter().collect();
    assert_eq!(unique.len(), spellings.len());
}

/// Build the admission ceiling the way `main` does and report the POST-clamp
/// limit the semaphore was constructed with.
fn admission_limit_for(value: usize) -> usize {
    Admission::new(AdmissionConfig {
        max_concurrent_scans: value,
        wait_budget: WaitBudget::Timeout(std::time::Duration::from_millis(1)),
    })
    .limit()
}

/// The startup event itself: the fields an operator greps for (issue #3225, D6).
mod startup_log {
    use super::*;

    /// One captured `tracing` event: its message plus its fields, rendered as
    /// strings. A field whose value recorded NOTHING (an `Option::None`) is
    /// absent from the map — which is exactly the "omitted on `Err`" contract.
    #[derive(Default)]
    struct CapturedFields(BTreeMap<String, String>);

    impl tracing::field::Visit for CapturedFields {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.0
                .insert(field.name().to_string(), format!("{value:?}"));
        }
        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            self.0.insert(field.name().to_string(), value.to_string());
        }
        fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
            self.0.insert(field.name().to_string(), value.to_string());
        }
        fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
            self.0.insert(field.name().to_string(), value.to_string());
        }
    }

    #[derive(Clone, Default)]
    struct Capture(Arc<Mutex<Vec<BTreeMap<String, String>>>>);

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Capture {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let mut fields = CapturedFields::default();
            event.record(&mut fields);
            if let Ok(mut events) = self.0.lock() {
                events.push(fields.0);
            }
        }
    }

    /// Emit the startup event for `argv` (parsed by the real parser) and return
    /// its fields.
    fn capture_startup(
        argv: &[&str],
        scans: Option<admission::ResolvedMaxConcurrentScans>,
    ) -> BTreeMap<String, String> {
        use tracing_subscriber::layer::SubscriberExt;

        let mut full = vec!["cqlite-flight", "--data-dir", "/tmp/cqlite-flight-test"];
        full.extend_from_slice(argv);
        let (args, matches) =
            Args::try_parse_with_matches_from(TEST_ALLOCATOR, full).expect("argv must parse");
        let scans = scans.unwrap_or_else(|| cli::resolve_max_concurrent_scans(&args, &matches));
        let limit = admission_limit_for(scans.value);

        let capture = Capture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        tracing::subscriber::with_default(subscriber, || {
            cli::log_startup(&args, &scans, limit, 1024, TEST_ALLOCATOR);
        });
        let events = capture.0.lock().expect("capture mutex").clone();
        let starting: Vec<_> = events
            .into_iter()
            .filter(|f| {
                f.get("message")
                    .is_some_and(|m| m.contains("cqlite-flight starting"))
            })
            .collect();
        assert_eq!(
            starting.len(),
            1,
            "exactly ONE `cqlite-flight starting` event must be emitted (no new log event)"
        );
        starting.into_iter().next().expect("one event")
    }

    #[test]
    #[serial]
    fn a_derived_ceiling_is_labelled_derived_with_its_input() {
        let fields = with_env(None, || capture_startup(&[], None));
        let p =
            probe_available_parallelism().expect("available_parallelism() answers on this host");
        assert_eq!(
            fields
                .get("max_concurrent_scans_source")
                .map(String::as_str),
            Some("derived")
        );
        assert_eq!(
            fields.get("available_parallelism").map(String::as_str),
            Some(p.to_string().as_str())
        );
        assert_eq!(
            fields.get("max_concurrent_scans").map(String::as_str),
            Some(derive_max_concurrent_scans(p).to_string().as_str())
        );
    }

    #[test]
    #[serial]
    fn a_flag_ceiling_is_labelled_flag() {
        let fields = with_env(None, || {
            capture_startup(&["--max-concurrent-scans", "7"], None)
        });
        assert_eq!(
            fields
                .get("max_concurrent_scans_source")
                .map(String::as_str),
            Some("flag")
        );
        assert_eq!(
            fields.get("max_concurrent_scans").map(String::as_str),
            Some("7")
        );
    }

    #[test]
    #[serial]
    fn an_environment_ceiling_is_labelled_env() {
        let fields = with_env(Some("11"), || capture_startup(&[], None));
        assert_eq!(
            fields
                .get("max_concurrent_scans_source")
                .map(String::as_str),
            Some("env")
        );
        assert_eq!(
            fields.get("max_concurrent_scans").map(String::as_str),
            Some("11")
        );
    }

    #[test]
    #[serial]
    fn an_unavailable_oracle_is_labelled_derived_fallback_and_omits_the_reading() {
        // The injected-`Err` arm, end to end through the real log event.
        let scans = admission::resolve_max_concurrent_scans(None, None);
        let fields = with_env(None, || capture_startup(&[], Some(scans)));
        assert_eq!(
            fields
                .get("max_concurrent_scans_source")
                .map(String::as_str),
            Some("derived-fallback")
        );
        assert!(
            !fields.contains_key("available_parallelism"),
            "the reading must be OMITTED when the oracle returned no answer, not logged as a \
             placeholder: {fields:?}"
        );
        assert_eq!(
            fields.get("max_concurrent_scans").map(String::as_str),
            Some(DEFAULT_MAX_CONCURRENT_SCANS.to_string().as_str())
        );
    }

    #[test]
    #[serial]
    fn the_logged_value_is_the_effective_post_clamp_ceiling() {
        // #2420 clamps a requested 0 up to 1; the log must report what the
        // semaphore was actually built with, not what was requested.
        let fields = with_env(None, || {
            capture_startup(&["--max-concurrent-scans", "0"], None)
        });
        assert_eq!(
            fields
                .get("max_concurrent_scans_source")
                .map(String::as_str),
            Some("flag")
        );
        assert_eq!(
            fields.get("max_concurrent_scans").map(String::as_str),
            Some("1"),
            "the logged ceiling is Admission::limit(), post-clamp"
        );
    }
}
