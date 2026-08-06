//! Runtime gate and window policy for the partition access-distribution probe
//! (issue #2827).
//!
//! Split out of `mod.rs` to keep each file inside the campsite-rule source target
//! (#1116): this file owns "is the probe on, and how long is a window", while
//! `mod.rs` owns the recorder, `table.rs` the counting structure, `types.rs` the
//! emitted vocabulary and `decision.rs` the procedure.

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::OnceLock;
use std::time::Duration;

/// Environment variable that turns the probe on. Default OFF.
const PROBE_ENV: &str = "CQLITE_PARTITION_ACCESS_PROBE";

/// Window length in seconds. Operator-reachable because the decision procedure's
/// remedy for a refused sample depends on it — see [`window_config_from_env`].
const WINDOW_SECS_ENV: &str = "CQLITE_PARTITION_ACCESS_WINDOW_SECS";

/// Window bound in recorded accesses.
const WINDOW_ACCESSES_ENV: &str = "CQLITE_PARTITION_ACCESS_WINDOW_ACCESSES";

/// Effective-state cache so the disabled hot path is ONE relaxed atomic load.
/// `0` = not yet resolved, `1` = on, `2` = off.
static EFFECTIVE: AtomicU8 = AtomicU8::new(0);
const STATE_UNRESOLVED: u8 = 0;
const STATE_ON: u8 = 1;
const STATE_OFF: u8 = 2;

/// Parse a `CQLITE_PARTITION_ACCESS_PROBE` value.
///
/// A pure function so the parse is unit-testable without touching the process
/// environment (the `now_clock::now_from` / `parse_read_path_mode` pattern).
/// Returns `None` for an unrecognised value — the caller reports that LOUDLY and
/// leaves the probe off, rather than silently treating a typo'd knob as "on".
pub fn parse_probe_flag(raw: &str) -> Option<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "on" | "yes" | "enabled" => Some(true),
        "0" | "false" | "off" | "no" | "disabled" | "" => Some(false),
        _ => None,
    }
}

/// The raw env value, read ONCE per process.
fn cached_env() -> Option<&'static str> {
    static ENV: OnceLock<Option<String>> = OnceLock::new();
    ENV.get_or_init(|| std::env::var(PROBE_ENV).ok()).as_deref()
}

fn resolve_from_env() -> bool {
    match cached_env() {
        None => false,
        Some(raw) => match parse_probe_flag(raw) {
            Some(v) => v,
            None => {
                // Loud, once (the env read itself is memoised, so this branch runs
                // at most once per process). A mistyped knob that silently no-ops
                // would defeat the knob's purpose.
                tracing::error!(
                    value = raw,
                    "unrecognised {PROBE_ENV} value — the partition access-distribution \
                     probe stays OFF; accepted values are 1/true/on/yes/enabled and \
                     0/false/off/no/disabled"
                );
                false
            }
        },
    }
}

/// Whether the probe is currently recording.
///
/// Steady-state cost is one relaxed atomic load. Off unless
/// `CQLITE_PARTITION_ACCESS_PROBE` says otherwise or a caller set
/// [`set_probe_enabled`].
#[inline]
pub fn enabled() -> bool {
    match EFFECTIVE.load(Ordering::Relaxed) {
        STATE_ON => true,
        STATE_OFF => false,
        _ => {
            let on = resolve_from_env();
            EFFECTIVE.store(if on { STATE_ON } else { STATE_OFF }, Ordering::Relaxed);
            on
        }
    }
}

/// Programmatically turn the probe on or off, taking precedence over the
/// environment (the `CQLITE_READ_PATH` config-over-env precedence pattern).
///
/// `Some(true)`/`Some(false)` pin the state; `None` returns the process to
/// resolving from the environment on the next [`enabled`] call.
pub fn set_probe_enabled(state: Option<bool>) {
    let v = match state {
        Some(true) => STATE_ON,
        Some(false) => STATE_OFF,
        None => STATE_UNRESOLVED,
    };
    EFFECTIVE.store(v, Ordering::Relaxed);
}

/// Production sampling-prefix cap. At `k = 20` the sample is 1-in-1,048,576, which
/// over a field-scale corpus admits a couple of keys — statistically worthless, so
/// the window is marked non-census and the decision procedure refuses it.
pub const DEFAULT_MAX_PREFIX_BITS: u32 = 20;

/// How a window's close was triggered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WindowConfig {
    /// Wall-clock length of a window. Checked on record; never asserted on by a
    /// correctness test.
    pub duration: Duration,
    /// Recorded-access bound; closes the window before the sample degrades on a
    /// workload far above the design rate.
    pub max_accesses: u64,
    /// Sampling-prefix cap. Once the recorder has widened the admission predicate
    /// this far and the table is STILL at its load factor, the surviving sample is
    /// too small to mean anything: the window is marked non-census and the decision
    /// procedure refuses it.
    ///
    /// Configurable only so the floor is reachable in a test. At the production
    /// default of [`DEFAULT_MAX_PREFIX_BITS`] the sample is 1-in-1,048,576, which
    /// no realistic corpus reaches — a property worth keeping, and a scenario worth
    /// being able to exercise.
    pub max_prefix_bits: u32,
}

impl Default for WindowConfig {
    fn default() -> Self {
        Self {
            duration: Duration::from_secs(60),
            max_accesses: 5_000_000,
            max_prefix_bits: DEFAULT_MAX_PREFIX_BITS,
        }
    }
}

/// The window policy an operator asked for, from the environment.
///
/// # Why this knob has to exist
///
/// The decision procedure REFUSES a window that is not a census, and its stated
/// remedy is "re-measure with a shorter window". With the window length reachable
/// only from Rust, that remedy would be unreachable in production: a field workload
/// touching more than the table's ~98k distinct partitions in the default 60 s would
/// be refused with nothing an operator could do about it — hollowing out the promise
/// that the verdict falls out of the first real workload.
///
/// Unset or unparseable values keep the default, loudly for the unparseable case: a
/// mistyped knob that silently no-ops defeats the knob's purpose. A zero or negative
/// duration is rejected the same way (it would close a window per access).
pub fn window_config_from_env() -> WindowConfig {
    let mut config = WindowConfig::default();
    if let Ok(raw) = std::env::var(WINDOW_SECS_ENV) {
        match raw.trim().parse::<u64>() {
            Ok(secs) if secs > 0 => config.duration = Duration::from_secs(secs),
            _ => tracing::error!(
                value = raw,
                "unrecognised {WINDOW_SECS_ENV} value — keeping the default window                  length; expected a positive whole number of seconds"
            ),
        }
    }
    if let Ok(raw) = std::env::var(WINDOW_ACCESSES_ENV) {
        match raw.trim().parse::<u64>() {
            Ok(n) if n > 0 => config.max_accesses = n,
            _ => tracing::error!(
                value = raw,
                "unrecognised {WINDOW_ACCESSES_ENV} value — keeping the default                  access bound; expected a positive whole number"
            ),
        }
    }
    config
}
