//! The parallelism-derived `do_get` admission default (issue #3225, §3).
//!
//! The shipped default was the compile-time constant
//! `DEFAULT_MAX_CONCURRENT_SCANS = 64` (issue #2420); #3217's sweep measured
//! peak throughput at a concurrency far below it on narrow servers, and
//! over-admission cost 16.4% throughput / a 31 s → 302 s p50 at one core. #3225
//! makes the default a function of the parallelism available to THIS PROCESS:
//!
//! ```text
//! N_default(P) = clamp(DERIVED_SCANS_PER_HARDWARE_THREAD * P,
//!                      MIN_DERIVED_MAX_CONCURRENT_SCANS,
//!                      DEFAULT_MAX_CONCURRENT_SCANS)
//!              = clamp(2 * P, 2, 64)
//! ```
//!
//! The formula is a PURE function of `P`, separated from the act of probing
//! `P`, precisely so it is exhaustively testable without a machine of the width
//! under test — that separation is the point, and this file is why.
//!
//! Two of the points below are MEASUREMENT-PINNED, not illustrations: `P=4 → 8`
//! and `P=8 → 16` are #3217's uncensored measured peaks (2 and 4 physical cores
//! on the SMT-on rig, `P = 2 × physical`). A future coefficient change that
//! breaks the fit fails here rather than passing a review.

use cqlite_flight::admission::{
    derive_max_concurrent_scans, probe_available_parallelism, AdmissionConfig,
    DEFAULT_MAX_CONCURRENT_SCANS, DERIVED_SCANS_PER_HARDWARE_THREAD,
    MIN_DERIVED_MAX_CONCURRENT_SCANS,
};

/// The documented table from the spec: `P ∈ {1,2,3,4,6,8,12,16,24,31,32,33,64,1024}`.
/// The floor binds at `P = 1`, the ceiling binds from `P = 32` upward.
const DOCUMENTED_TABLE: &[(usize, usize)] = &[
    (1, 2),
    (2, 4),
    (3, 6),
    (4, 8),
    (6, 12),
    (8, 16),
    (12, 24),
    (16, 32),
    (24, 48),
    (31, 62),
    (32, 64),
    (33, 64),
    (64, 64),
    (1024, 64),
];

#[test]
fn the_derivation_matches_the_documented_table() {
    for &(p, expected) in DOCUMENTED_TABLE {
        assert_eq!(
            derive_max_concurrent_scans(p),
            expected,
            "clamp(2 * P, 2, 64) at P={p} must be {expected}"
        );
    }
}

#[test]
fn the_two_measurement_pinned_points_are_exact() {
    // #3217 measured peak throughput at N=8 on 2 physical cores (P=4) and N=16
    // on 4 physical cores (P=8) — the two UNCENSORED points of the ramp. The
    // formula reproduces both exactly; nothing here is rounded toward them.
    assert_eq!(
        derive_max_concurrent_scans(4),
        8,
        "P=4 (2 physical cores, SMT on) is #3217's measured peak N=8"
    );
    assert_eq!(
        derive_max_concurrent_scans(8),
        16,
        "P=8 (4 physical cores, SMT on) is #3217's measured peak N=16"
    );
}

#[test]
fn the_derivation_is_monotone_non_decreasing() {
    let mut previous = derive_max_concurrent_scans(0);
    for p in 1..=256usize {
        let current = derive_max_concurrent_scans(p);
        assert!(
            current >= previous,
            "derivation must be monotone non-decreasing: P={p} gave {current} after {previous}"
        );
        previous = current;
    }
}

#[test]
fn the_floor_binds_so_a_one_cpu_quota_never_serialises_every_scan() {
    // A single-permit server serialises every scan, and #3217 measured N=1 as
    // the WORST point at every width. P=0 is not reachable through
    // `available_parallelism` (it returns a `NonZeroUsize`) but the pure
    // function must still be total.
    assert_eq!(
        derive_max_concurrent_scans(0),
        MIN_DERIVED_MAX_CONCURRENT_SCANS
    );
    assert_eq!(
        derive_max_concurrent_scans(1),
        MIN_DERIVED_MAX_CONCURRENT_SCANS
    );
    assert_eq!(MIN_DERIVED_MAX_CONCURRENT_SCANS, 2);
}

#[test]
fn the_ceiling_binds_so_the_change_is_one_directional() {
    // No host is admitted more widely than it is on the pre-#3225 release: the
    // derived value never exceeds the constant, including at a width larger
    // than any real machine (where `2 * P` would overflow a naive multiply).
    for p in [32usize, 33, 64, 1024, usize::MAX / 2, usize::MAX] {
        let derived = derive_max_concurrent_scans(p);
        assert!(
            derived <= DEFAULT_MAX_CONCURRENT_SCANS,
            "derived {derived} at P={p} must not exceed the {DEFAULT_MAX_CONCURRENT_SCANS} ceiling"
        );
    }
    assert_eq!(
        derive_max_concurrent_scans(usize::MAX),
        DEFAULT_MAX_CONCURRENT_SCANS
    );
    assert_eq!(DEFAULT_MAX_CONCURRENT_SCANS, 64);
    assert_eq!(DERIVED_SCANS_PER_HARDWARE_THREAD, 2);
}

#[test]
fn the_config_default_is_the_derivation_applied_to_this_host() {
    // The probe is `std::thread::available_parallelism()`, which on this host
    // always answers; assert the wiring rather than a host-specific number.
    let p = probe_available_parallelism()
        .expect("available_parallelism() answers on every platform this suite runs on");
    assert_eq!(
        AdmissionConfig::default().max_concurrent_scans,
        derive_max_concurrent_scans(p),
        "AdmissionConfig::default() must be the derived value, not the 64 constant"
    );
}

/// `AdmissionConfig::from_env()` keeps its "a present-but-unparseable value
/// falls back rather than failing startup" contract and its `>= 1` filter — the
/// only change is that its FALLBACK TARGET is now the derived value, not the 64
/// constant (issue #3225, §3).
///
/// These tests mutate the process environment, so they are `#[serial]`.
mod from_env {
    use super::*;
    use cqlite_flight::admission::ENV_MAX_CONCURRENT_SCANS;
    use serial_test::serial;

    /// Run `body` with `ENV_MAX_CONCURRENT_SCANS` set to `value` (or removed
    /// when `None`), restoring the previous state afterwards.
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

    fn derived_here() -> usize {
        derive_max_concurrent_scans(
            probe_available_parallelism().expect("available_parallelism() answers on this host"),
        )
    }

    #[test]
    #[serial]
    fn an_unset_environment_yields_the_derived_value() {
        let cfg = with_env(None, AdmissionConfig::from_env);
        assert_eq!(cfg.max_concurrent_scans, derived_here());
    }

    #[test]
    #[serial]
    fn a_set_environment_value_wins_over_the_derived_value() {
        let cfg = with_env(Some("7"), AdmissionConfig::from_env);
        assert_eq!(cfg.max_concurrent_scans, 7);
    }

    #[test]
    #[serial]
    fn an_unparseable_value_falls_back_to_the_derived_value_rather_than_failing() {
        let cfg = with_env(Some("not-a-number"), AdmissionConfig::from_env);
        assert_eq!(cfg.max_concurrent_scans, derived_here());
    }

    #[test]
    #[serial]
    fn a_zero_ceiling_falls_back_to_the_derived_value() {
        // The pre-existing `>= 1` filter is preserved: a zero ceiling would be a
        // semaphore that rejects everything.
        let cfg = with_env(Some("0"), AdmissionConfig::from_env);
        assert_eq!(cfg.max_concurrent_scans, derived_here());
    }
}

/// AC2's structural guard: the derivation path must never be re-pointed at HOST
/// topology.
///
/// Deliberately STRUCTURAL rather than behavioural. The failure mode is a
/// future edit swapping the container-correct probe for
/// `num_cpus::get_physical()` (which parses `/proc/cpuinfo` and applies neither
/// the cgroup quota nor the affinity mask, so a 1-CPU pod on a 96-core node
/// would derive the MAXIMUM ceiling on the NARROWEST server). Catching that
/// behaviourally needs a container; catching it in the source needs only this.
mod host_topology_guard {
    /// Every file on the admission-default derivation path.
    const DERIVATION_PATH: &[&str] = &["src/admission.rs", "src/cli.rs", "src/main.rs"];

    /// Tokens that would mean the derivation reads host topology instead of the
    /// parallelism available to this process.
    const FORBIDDEN: &[&str] = &["/proc/cpuinfo", "/sys/devices/system/cpu", "get_physical"];

    /// Whole-line comments are exempt so the source can NAME the disqualified
    /// API in its prohibition note (the note is the documentation a future
    /// editor reads). A TRAILING comment is deliberately NOT exempt: exempting
    /// it would need to distinguish comment text from code text mid-line, and
    /// the cost of that rule is one style constraint — put the note on its own
    /// line.
    fn is_comment_line(line: &str) -> bool {
        line.trim_start().starts_with("//")
    }

    #[test]
    fn the_derivation_path_reads_no_host_topology() {
        let crate_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let mut offences = Vec::new();
        for relative in DERIVATION_PATH {
            let path = crate_root.join(relative);
            let source = std::fs::read_to_string(&path).unwrap_or_else(|e| {
                panic!("derivation-path file {} is unreadable: {e}", path.display())
            });
            for (index, line) in source.lines().enumerate() {
                if is_comment_line(line) {
                    continue;
                }
                for token in FORBIDDEN {
                    if line.contains(token) {
                        offences.push(format!(
                            "{}:{}: forbidden host-topology token `{token}` in `{}`",
                            relative,
                            index + 1,
                            line.trim()
                        ));
                    }
                }
            }
        }
        assert!(
            offences.is_empty(),
            "the admission-default derivation path must read the parallelism available to THIS \
             PROCESS (std::thread::available_parallelism), never host topology (issue #3225, AC2):\n{}",
            offences.join("\n")
        );
    }

    #[test]
    fn the_guard_reads_the_files_it_claims_to_read() {
        // Affirmative measurement (CLAUDE.md): a guard whose file list has gone
        // stale would pass by inspecting nothing. Assert each named file exists
        // AND that the token scan can actually fire.
        let crate_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        for relative in DERIVATION_PATH {
            let path = crate_root.join(relative);
            assert!(
                path.is_file(),
                "derivation-path file {} is missing — the guard would inspect nothing",
                path.display()
            );
        }
        assert!(!FORBIDDEN.is_empty());
        assert!(!is_comment_line("    let n = num_cpus::get_physical();"));
        assert!(is_comment_line("    // never num_cpus::get_physical()"));
    }
}
