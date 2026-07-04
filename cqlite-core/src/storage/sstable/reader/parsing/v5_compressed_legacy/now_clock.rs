//! Issue #1741 / #1853: the read-time TTL "now" clock.
//!
//! Extracted from `mod.rs` (campsite rule / file-size ratchet, epic #1116) so
//! the parser module doesn't grow past the source-file threshold.

/// Issue #1853: `#[doc(hidden)]` test-only seam to pin the read-time TTL "now"
/// clock deterministically. When `CQLITE_TTL_NOW_OVERRIDE_SECS` holds a valid
/// `i64` (epoch seconds), it overrides the wall clock for read-time TTL expiry
/// so parity tests can read a long-expired fixture "as of" its capture time.
/// An unset or malformed value is IGNORED (falls back to the wall clock) and
/// never panics — this is a library-code path.
const TTL_NOW_OVERRIDE_ENV: &str = "CQLITE_TTL_NOW_OVERRIDE_SECS";

/// Issue #1741: current wall-clock as epoch seconds, for read-time TTL expiry.
/// Falls back to `0` (nothing appears expired) if the clock is before the epoch.
///
/// Issue #1853: honors the `CQLITE_TTL_NOW_OVERRIDE_SECS` test seam first; an
/// invalid/absent override leaves the wall-clock behavior unchanged.
pub(super) fn now_epoch_secs() -> i64 {
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

    // #[serial]: these three tests mutate the process-global
    // CQLITE_TTL_NOW_OVERRIDE_SECS env var; without serialization cargo's
    // intra-binary parallel test threads could race each other's set/remove.

    #[test]
    #[serial]
    fn override_env_pins_now() {
        std::env::set_var(TTL_NOW_OVERRIDE_ENV, "1759716000");
        let got = now_epoch_secs();
        std::env::remove_var(TTL_NOW_OVERRIDE_ENV);
        assert_eq!(got, 1_759_716_000, "override must be honored verbatim");
    }

    #[test]
    #[serial]
    fn invalid_override_falls_back_to_wall_clock() {
        std::env::set_var(TTL_NOW_OVERRIDE_ENV, "not-a-number");
        let got = now_epoch_secs();
        std::env::remove_var(TTL_NOW_OVERRIDE_ENV);
        // Wall clock in 2026+ is comfortably past this override candidate.
        assert!(
            got > 1_759_716_000,
            "an invalid override must fall back to the wall clock, got {got}"
        );
    }

    #[test]
    #[serial]
    fn absent_override_falls_back_to_wall_clock() {
        std::env::remove_var(TTL_NOW_OVERRIDE_ENV);
        let got = now_epoch_secs();
        assert!(
            got > 1_759_716_000,
            "no override set must fall back to the wall clock, got {got}"
        );
    }
}
