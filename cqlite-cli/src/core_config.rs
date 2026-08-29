//! CLI configuration → core (`cqlite_core::Config`) translation.
//!
//! Split out of `main.rs` (issue #1695) so the mapping is a LIBRARY surface an
//! end-to-end test can exercise: `main.rs` is a binary target, so anything living
//! there is unreachable from an integration test, and a knob whose wiring cannot
//! be tested is a knob that silently rots. The binary's `create_core_config` is
//! now a one-line delegation to [`to_core_config`].
//!
//! # The wired knobs
//!
//! | CLI (`[performance]`)  | core field                          |
//! |------------------------|-------------------------------------|
//! | `memory_limit_mb`      | `memory.max_memory`                 |
//! | `cache_size_mb`        | `memory.block_cache.max_size`       |
//! | `query_timeout_ms`     | `query.max_execution_time` (#1695)  |
//!
//! `query_timeout_ms` is the operator's query execution budget, ENFORCED at the
//! query-engine chokepoint (`cqlite_core::query::engine::deadline`). **`0` means
//! no timeout** — it maps to `Duration::ZERO`, the core's documented unbounded
//! sentinel.

use anyhow::Result;
use cqlite_core::Config as CoreConfig;

use crate::config::Config as CliConfig;

/// Convert CLI configuration to core database configuration.
///
/// The returned config is already `validate()`d, so a caller can hand it
/// straight to `Database::open`.
pub fn to_core_config(cli_config: &CliConfig) -> Result<CoreConfig> {
    let mut core_config = CoreConfig::default();

    // Apply CLI configuration settings to core config
    if let Some(memory_limit_mb) = cli_config.performance.memory_limit_mb {
        core_config.memory.max_memory = memory_limit_mb * 1024 * 1024; // Convert MB to bytes
    }

    // Set cache size from CLI config
    core_config.memory.block_cache.max_size = cli_config.performance.cache_size_mb * 1024 * 1024; // Convert MB to bytes

    // Set the query execution budget (issue #1695). `query_timeout_ms = 0` maps to
    // `Duration::ZERO`, the core's documented "no timeout" sentinel.
    core_config.query.max_execution_time =
        std::time::Duration::from_millis(cli_config.performance.query_timeout_ms);

    // Validate the configuration
    core_config
        .validate()
        .map_err(|e| anyhow::anyhow!("Invalid database configuration: {}", e))?;

    Ok(core_config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// The CLI knob must land on the field the engine actually enforces — the
    /// unit conversion included (issue #1695).
    #[test]
    fn query_timeout_ms_maps_to_max_execution_time() {
        let mut cli = CliConfig::default();
        cli.performance.query_timeout_ms = 1;
        let core = to_core_config(&cli).expect("valid config");
        assert_eq!(core.query.max_execution_time, Duration::from_millis(1));

        cli.performance.query_timeout_ms = 30_000;
        let core = to_core_config(&cli).expect("valid config");
        assert_eq!(core.query.max_execution_time, Duration::from_secs(30));
    }

    /// `0` is the documented "no timeout" spelling of the knob and must reach the
    /// core as `Duration::ZERO` (its unbounded sentinel), not as an instantly
    /// expiring budget or a validation error.
    #[test]
    fn zero_query_timeout_ms_is_the_unbounded_sentinel() {
        let mut cli = CliConfig::default();
        cli.performance.query_timeout_ms = 0;
        let core = to_core_config(&cli).expect("0 must be a VALID configuration");
        assert_eq!(core.query.max_execution_time, Duration::ZERO);
    }

    /// The shipped CLI default (30s) must survive the mapping.
    #[test]
    fn shipped_default_is_thirty_seconds() {
        let core = to_core_config(&CliConfig::default()).expect("valid config");
        assert_eq!(core.query.max_execution_time, Duration::from_secs(30));
    }
}
