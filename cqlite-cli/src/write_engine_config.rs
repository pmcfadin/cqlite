//! The CLI's PUBLIC write-path configuration, validated (issue #1697).
//!
//! The CLI builds its `WriteEngineConfig` through the one canonical bridge,
//! `WriteEngineConfig::from_config`, so everything it wants the write engine to
//! honour must be expressed on the public [`cqlite_core::Config`] — and must
//! satisfy that config's own rules.
//!
//! Nothing on the write path enforces those rules for us: `from_config` is
//! infallible by design (it is also `WriteEngineConfig::new`'s definition, so
//! making it fallible would ripple through every construction site), and
//! `Database::open` does not validate either. So the CLI validates here, at the
//! point where it assembles the config.
//!
//! The rule that bites is `memtable_hard_limit > memtable_size_threshold`:
//! `CQLITE_MEMTABLE_FLUSH_THRESHOLD=300000000` asks for a 300MB flush threshold
//! above the 256MB admission ceiling, which wedges the engine permanently —
//! auto-flush never fires, and `check_admission` rejects every write once the
//! memtable passes 256MB. Before this validation the CLI accepted it silently.
//!
//! Enforcement posture across the four write paths is uneven and deliberately
//! recorded rather than papered over; see `WriteEngineConfig::from_config`.

use anyhow::{anyhow, Result};

/// Byte-count override for the memtable flush threshold (issue #1693).
///
/// Makes an interactive session's auto-flush observable without writing 64MB.
/// Unset in production.
pub const FLUSH_THRESHOLD_ENV: &str = "CQLITE_MEMTABLE_FLUSH_THRESHOLD";

/// Resolve the validated public config the CLI's write engine runs on.
pub fn resolve() -> Result<cqlite_core::Config> {
    resolve_from(std::env::var(FLUSH_THRESHOLD_ENV).ok().as_deref())
}

/// `resolve` with the environment lifted into a parameter, so the rules below are
/// testable without mutating process-global state from a test.
fn resolve_from(flush_threshold_raw: Option<&str>) -> Result<cqlite_core::Config> {
    let mut config = cqlite_core::Config::default();

    // An empty/whitespace value reads as "not set" — the common way to neutralise
    // an exported variable — but genuine garbage is an ERROR rather than a silent
    // fallback to the default. A knob that quietly does nothing is the exact
    // defect class #1697 exists to remove.
    if let Some(raw) = flush_threshold_raw.map(str::trim).filter(|r| !r.is_empty()) {
        // Parsed as `u64` to match the public knob's own width. Parsing `usize`
        // would silently reject every value above `u32::MAX` on a 32-bit host,
        // and then need a cast back.
        let bytes: u64 = raw
            .parse()
            .map_err(|_| anyhow!("{FLUSH_THRESHOLD_ENV}: expected a byte count, got {raw:?}"))?;
        config.storage.memtable_size_threshold = bytes;
    }

    config
        .validate()
        .map_err(|e| anyhow!("invalid write configuration: {e}"))?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_and_empty_overrides_yield_the_public_defaults() {
        let defaults = cqlite_core::Config::default();
        for raw in [None, Some(""), Some("   ")] {
            let resolved = resolve_from(raw).expect("defaults must validate");
            assert_eq!(
                resolved.storage.memtable_size_threshold, defaults.storage.memtable_size_threshold,
                "{raw:?} must read as unset"
            );
        }
    }

    #[test]
    fn a_valid_override_reaches_the_public_knob() {
        let resolved = resolve_from(Some(" 4096 ")).expect("4096 bytes is valid");
        assert_eq!(resolved.storage.memtable_size_threshold, 4096);
    }

    /// The wedge this module exists to stop: a threshold above the 256MB
    /// admission ceiling is never flushed AND rejected at admission, so the
    /// write path dead-ends permanently. Previously accepted in silence.
    #[test]
    fn an_override_above_the_hard_limit_is_rejected() {
        let hard_limit = cqlite_core::Config::default().storage.memtable_hard_limit;
        let err = resolve_from(Some("300000000"))
            .expect_err("300MB > the 256MB hard limit must not be accepted")
            .to_string();
        assert!(err.contains("invalid write configuration"), "{err}");
        // The message must name both colliding values (#1697 item 4).
        assert!(err.contains(&hard_limit.to_string()), "{err}");
        assert!(err.contains("300000000"), "{err}");
    }

    #[test]
    fn a_zero_override_is_rejected_rather_than_flushing_per_write() {
        let err = resolve_from(Some("0"))
            .expect_err("0 would make should_flush(0) true after every write")
            .to_string();
        assert!(err.contains("memtable_size_threshold"), "{err}");
    }

    #[test]
    fn garbage_is_an_error_not_a_silent_fallback() {
        for raw in ["64MB", "-1", "1.5", "eleven"] {
            let err = resolve_from(Some(raw))
                .expect_err("a malformed byte count must be surfaced")
                .to_string();
            assert!(err.contains(FLUSH_THRESHOLD_ENV), "{err}");
            assert!(err.contains(raw), "{err}");
        }
    }
}
