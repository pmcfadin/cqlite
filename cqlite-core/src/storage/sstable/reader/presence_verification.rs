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
//!
//! # Config plumbing + precedence (issue #2163, roborev r4)
//!
//! [`crate::observability::ObservabilityConfig::verify_presence_oracle`] is a
//! PROGRAMMATIC way to set this switch (e.g. `ObservabilityConfig::builder()
//! .verify_presence_oracle(true).build()`), for callers that construct config in
//! code rather than through the environment. [`crate::observability::init`] calls
//! [`apply_config`] with that field so the config value actually reaches this
//! switch — it is not merely decorative. Precedence (conventional
//! env-overrides-config, matching every other `ObservabilityConfig` field's own
//! `from_env` behaviour): if [`ENV_VAR`] is explicitly set to a parseable boolean,
//! it ALWAYS wins over the config value; otherwise the config value applies.

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

/// Apply an [`ObservabilityConfig`](crate::observability::ObservabilityConfig)-
/// sourced enablement value to this runtime switch (issue #2163, roborev r4 —
/// closes the "config field is decorative" gap).
///
/// [`ENV_VAR`], when explicitly set to a parseable boolean, ALWAYS takes
/// precedence over `config_value` — the conventional env-overrides-config rule
/// [`crate::observability::ObservabilityConfig::from_env`] already applies to
/// every other field. Otherwise `config_value` becomes the switch's state. Called
/// from [`crate::observability::init`] (both the `observability`-feature-enabled
/// and inert builds) so a programmatic
/// `ObservabilityConfig::builder().verify_presence_oracle(true).build()` actually
/// flips the switch the read path consults. Idempotent and safe to call multiple
/// times (e.g. across repeated `init` calls in a test process) — it always
/// recomputes from scratch and FORCES the result into the shared state, so a
/// later `init` call can still correct an earlier one.
pub(crate) fn apply_config(config_value: bool) {
    let env_value = std::env::var(ENV_VAR).ok().and_then(|v| parse_bool(&v));
    let effective = env_value.unwrap_or(config_value);
    STATE.store(if effective { ON } else { OFF }, Ordering::Release);
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
