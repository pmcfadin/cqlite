//! Issue #1741 / #1853: the read-time TTL "now" clock.
//!
//! Extracted from `mod.rs` (campsite rule / file-size ratchet, epic #1116) so
//! the parser module doesn't grow past the source-file threshold.

/// Issue #1853 roborev round 3: ALL of the override-parsing/fallback logic as
/// a PURE function — no env access, no globals. This makes the override path
/// unit-testable via plain argument passing instead of process-global
/// `std::env::set_var`/`remove_var`, which is unsound to share with any other
/// test in the same `cqlite-core` lib-test binary that constructs a
/// `V5CompressedLegacyParser` in parallel (e.g. `block_entries.rs`,
/// `frozen.rs`) — a pure function makes that race unrepresentable rather than
/// merely guarded (roborev findings 1435-Low-1 and 1436-Medium both addressed
/// structurally, not procedurally).
///
/// `raw_override`, when `Some` and parseable as an `i64` (epoch seconds), is
/// honored verbatim (bypassing the wall clock entirely) — mirrors
/// `now_epoch_secs()`'s prior "valid override always wins" behavior. `None`
/// or an unparseable string falls back to `wall_clock_secs`.
fn now_from(raw_override: Option<&str>, wall_clock_secs: i64) -> i64 {
    raw_override
        .and_then(|raw| raw.trim().parse::<i64>().ok())
        .unwrap_or(wall_clock_secs)
}

/// Current wall clock as epoch seconds, saturating to `0` (nothing appears
/// expired) if the clock is somehow before the Unix epoch.
fn wall_clock_now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

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
/// Reading this env var is only ever done here, from the separate-process
/// `issue_694_writetime_ttl_parity.rs` integration test binary (whose own
/// `EnvVarGuard` mutation therefore races nothing in-process) — never from a
/// `cqlite-core` unit test, which instead exercises the override/parse/
/// fallback logic directly via the pure `now_from()` above (roborev round 3).
#[cfg(debug_assertions)]
const TTL_NOW_OVERRIDE_ENV: &str = "CQLITE_TTL_NOW_OVERRIDE_SECS";

/// Issue #1741: current wall-clock as epoch seconds, for read-time TTL expiry.
/// Falls back to `0` (nothing appears expired) if the clock is before the epoch.
///
/// Issue #1853: in debug builds, honors the `CQLITE_TTL_NOW_OVERRIDE_SECS` test
/// seam first; an invalid/absent override leaves the wall-clock behavior
/// unchanged. In release builds this is a straight `SystemTime::now()` call —
/// the override seam (including the env read itself) is compiled out entirely.
pub(crate) fn now_epoch_secs() -> i64 {
    #[cfg(debug_assertions)]
    let raw_override = std::env::var(TTL_NOW_OVERRIDE_ENV).ok();
    #[cfg(not(debug_assertions))]
    let raw_override: Option<String> = None;

    now_from(raw_override.as_deref(), wall_clock_now_secs())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Issue #1853 roborev round 3: these tests call the PURE `now_from()`
    // directly with an explicit override argument — no env::set_var/remove_var,
    // no guard type, no #[serial]. There is no process-global mutation here for
    // any parallel test (in this binary or any other) to race.

    #[test]
    fn override_pins_now() {
        let got = now_from(Some("1759716000"), wall_clock_now_secs());
        assert_eq!(got, 1_759_716_000, "override must be honored verbatim");
    }

    #[test]
    fn invalid_override_falls_back_to_wall_clock() {
        let wall = wall_clock_now_secs();
        let got = now_from(Some("not-a-number"), wall);
        assert_eq!(
            got, wall,
            "an invalid override must fall back to the wall clock"
        );
    }

    #[test]
    fn absent_override_falls_back_to_wall_clock() {
        let wall = wall_clock_now_secs();
        let got = now_from(None, wall);
        assert_eq!(
            got, wall,
            "no override set must fall back to the wall clock"
        );
    }
}
