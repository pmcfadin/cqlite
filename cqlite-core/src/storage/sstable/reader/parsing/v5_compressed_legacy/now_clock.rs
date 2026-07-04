//! Issue #1741 / #1853: the read-time TTL "now" clock.
//!
//! Extracted from `mod.rs` (campsite rule / file-size ratchet, epic #1116) so
//! the parser module doesn't grow past the source-file threshold.

/// Issue #1853: test-only seam to pin the read-time TTL "now" clock
/// deterministically. When `CQLITE_TTL_NOW_OVERRIDE_SECS` holds a valid `i64`
/// (epoch seconds), it overrides the wall clock for read-time TTL expiry so
/// parity tests can read a long-expired fixture "as of" its capture time. An
/// unset or malformed value is IGNORED (falls back to the wall clock) and
/// never panics — this is a library-code path.
///
/// Gated `#[cfg(debug_assertions)]` (roborev #1853 finding 1): this is a raw
/// env-var read with no feature plumbing, so it must never be reachable from a
/// real user's release build — a `--release` process that happens to inherit
/// this variable from its environment must not have its TTL-expiry decisions
/// silently overridden. `debug_assertions` is on for every `cargo test`/`cargo
/// build` (debug) invocation, including the agent gate, so the seam and its
/// coverage are always exercised in CI; it compiles out entirely (dead code,
/// zero cost, zero env read) in `--release`. A Cargo feature was considered
/// and rejected: a `required-features` integration test silently *skips* when
/// the feature is off, which recreates the exact dead-coverage failure #1853
/// exists to fix. Reusing the `experimental` feature was also rejected — it
/// would muddy that flag's no-heuristics meaning.
///
/// Invariant: any test that sets this var must be `#[serial]` (see the `tests`
/// module below) — it is a process-global mutation and races with any other
/// test in the same binary that constructs a `V5CompressedLegacyParser`
/// in parallel.
#[cfg(debug_assertions)]
const TTL_NOW_OVERRIDE_ENV: &str = "CQLITE_TTL_NOW_OVERRIDE_SECS";

/// Issue #1741: current wall-clock as epoch seconds, for read-time TTL expiry.
/// Falls back to `0` (nothing appears expired) if the clock is before the epoch.
///
/// Issue #1853: in debug builds, honors the `CQLITE_TTL_NOW_OVERRIDE_SECS` test
/// seam first; an invalid/absent override leaves the wall-clock behavior
/// unchanged. In release builds this is a straight `SystemTime::now()` call —
/// the override seam is compiled out entirely.
pub(super) fn now_epoch_secs() -> i64 {
    #[cfg(debug_assertions)]
    if let Ok(raw) = std::env::var(TTL_NOW_OVERRIDE_ENV) {
        if let Ok(override_secs) = raw.trim().parse::<i64>() {
            return override_secs;
        }
    }
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    // #[serial]: these tests mutate the process-global CQLITE_TTL_NOW_OVERRIDE_SECS
    // env var; without serialization cargo's intra-binary parallel test threads
    // (including other test files, e.g. regression_1741h_tests.rs, that construct
    // a V5CompressedLegacyParser) could race each other's set/remove and observe
    // an unexpectedly pinned clock. Invariant: any test anywhere in this crate
    // that sets CQLITE_TTL_NOW_OVERRIDE_SECS must be #[serial].

    /// RAII guard (roborev #1853 finding 2): saves whatever value (or absence)
    /// preceded the override, sets the new value, and restores the prior state
    /// on `Drop` — including on a panicking assertion unwind, unlike a
    /// non-RAII "restore after f()" helper which would leak the override into
    /// every subsequent test in the binary if the closure panicked. Never
    /// clobbers a value a developer's shell may have exported outside the
    /// test process. Mirrors the `EnvVarGuard` pattern in
    /// `issue_694_writetime_ttl_parity.rs`.
    #[cfg(debug_assertions)]
    struct OverrideGuard {
        prior: Option<String>,
    }

    #[cfg(debug_assertions)]
    impl OverrideGuard {
        fn set(value: &str) -> Self {
            let prior = std::env::var(TTL_NOW_OVERRIDE_ENV).ok();
            std::env::set_var(TTL_NOW_OVERRIDE_ENV, value);
            Self { prior }
        }
    }

    #[cfg(debug_assertions)]
    impl Drop for OverrideGuard {
        fn drop(&mut self) {
            match self.prior.take() {
                Some(v) => std::env::set_var(TTL_NOW_OVERRIDE_ENV, v),
                None => std::env::remove_var(TTL_NOW_OVERRIDE_ENV),
            }
        }
    }

    #[test]
    #[serial]
    #[cfg(debug_assertions)]
    fn override_env_pins_now() {
        let _guard = OverrideGuard::set("1759716000");
        let got = now_epoch_secs();
        assert_eq!(
            got, 1_759_716_000,
            "override must be honored verbatim under debug_assertions"
        );
    }

    #[test]
    #[serial]
    #[cfg(debug_assertions)]
    fn invalid_override_falls_back_to_wall_clock() {
        let _guard = OverrideGuard::set("not-a-number");
        let got = now_epoch_secs();
        // Wall clock in 2026+ is comfortably past this override candidate.
        assert!(
            got > 1_759_716_000,
            "an invalid override must fall back to the wall clock, got {got}"
        );
    }

    #[test]
    #[serial]
    fn absent_override_falls_back_to_wall_clock() {
        #[cfg(debug_assertions)]
        std::env::remove_var(TTL_NOW_OVERRIDE_ENV);
        let got = now_epoch_secs();
        assert!(
            got > 1_759_716_000,
            "no override set must fall back to the wall clock, got {got}"
        );
    }
}
