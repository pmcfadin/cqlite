//! Opt-in, default-OFF presence-oracle false-negative verification switch
//! (issue #2163).
//!
//! A presence oracle (the BIG bloom filter / BTI Partitions.db trie) must NEVER
//! report a false negative: a "definitely absent" verdict for a key that is
//! actually present would silently drop data. This switch turns on an
//! AUTHORITATIVE confirmation scan of an SSTable whenever its oracle reports a
//! key absent; a contradiction increments `cqlite.read.bloom.false_negatives`
//! (see [`crate::observability::catalog::READ_BLOOM_FALSE_NEGATIVES`]). Under a
//! correct oracle the counter stays 0 — a non-zero value is a corruption alarm.
//!
//! It is **off by default** and gated by an explicit runtime switch, because the
//! confirmation scan is the one presence-oracle counter that costs real work. The
//! switch is read once from the `CQLITE_VERIFY_PRESENCE_ORACLE` environment
//! variable (booleans: `1/0`, `true/false`, `yes/no`, `on/off`), then cached. The
//! `observability-testing`-gated [`set_enabled_for_testing`] lets the correctness
//! tests toggle it deterministically without touching process env.

use std::sync::atomic::{AtomicU8, Ordering};

const UNINIT: u8 = 0;
const OFF: u8 = 1;
const ON: u8 = 2;

/// Tri-state cache: `UNINIT` until first resolved from the environment, then
/// pinned to `OFF`/`ON`. Test overrides store `OFF`/`ON` directly.
static STATE: AtomicU8 = AtomicU8::new(UNINIT);

/// Environment variable that enables the opt-in verification when truthy.
pub const ENV_VAR: &str = "CQLITE_VERIFY_PRESENCE_ORACLE";

fn parse_bool(s: &str) -> Option<bool> {
    match s.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

/// Whether presence-oracle false-negative verification is enabled.
///
/// Resolves lazily from [`ENV_VAR`] on first call (default `false` when unset or
/// unparseable), caching the result so subsequent reads are a single relaxed
/// atomic load. Always `false` unless explicitly turned on — so the default
/// production read path performs no confirmation scan and costs nothing beyond
/// the existing presence check.
pub fn enabled() -> bool {
    match STATE.load(Ordering::Acquire) {
        OFF => false,
        ON => true,
        _ => {
            let value = std::env::var(ENV_VAR)
                .ok()
                .and_then(|v| parse_bool(&v))
                .unwrap_or(false);
            let encoded = if value { ON } else { OFF };
            // Only the first resolver wins; a concurrent test override that already
            // pinned the state is respected (compare_exchange fails, we re-read).
            match STATE.compare_exchange(UNINIT, encoded, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => value,
                Err(existing) => existing == ON,
            }
        }
    }
}

/// Force the switch on/off for tests (issue #2163). Gated behind
/// `observability-testing` so production builds never expose a runtime mutator;
/// integration tests compile the library with that feature and toggle the switch
/// deterministically around a flow.
#[cfg(feature = "observability-testing")]
pub fn set_enabled_for_testing(enabled: bool) {
    STATE.store(if enabled { ON } else { OFF }, Ordering::Release);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_bool_recognizes_bounded_forms() {
        for t in ["1", "true", "TRUE", "yes", "on"] {
            assert_eq!(parse_bool(t), Some(true), "{t}");
        }
        for f in ["0", "false", "no", "off"] {
            assert_eq!(parse_bool(f), Some(false), "{f}");
        }
        assert_eq!(parse_bool("maybe"), None);
    }

    #[cfg(feature = "observability-testing")]
    #[test]
    fn test_override_toggles_state() {
        set_enabled_for_testing(true);
        assert!(enabled());
        set_enabled_for_testing(false);
        assert!(!enabled());
    }
}
