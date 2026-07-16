//! Read-path forcing knob (issue #1918).
//!
//! A single gate that wraps [`classify_partition_lookup`](super::classify_partition_lookup)'s
//! outcome so an operator can force a `SELECT` down a specific access path for
//! **test/debug** purposes — never a performance recommendation. The knob:
//!
//! - [`ReadPathMode::Auto`] (default): today's behavior, byte-for-byte. An unset
//!   knob adds no per-query cost beyond one relaxed `OnceLock` load.
//! - [`ReadPathMode::Point`]: **fail closed** with
//!   [`Error::ForcedReadPathUnavailable`] whenever the executor would not run a
//!   genuinely partition-targeted lookup — never a silent full scan.
//! - [`ReadPathMode::Full`]: force the full-scan + reconciliation path regardless
//!   of classification, recording [`FallbackReason::ForcedFullScan`].
//!
//! Forcing governs **routing only** — values, tombstone shadowing, timestamp
//! resolution, and WRITETIME/TTL are byte-identical across all three modes — and
//! the mode is taken solely from explicit config/env, never inferred from data
//! bytes (no-heuristics mandate, #28).
//!
//! # Resolution
//!
//! The mode is resolved config-over-env-over-`Auto`:
//! 1. an explicit [`crate::config::QueryConfig::forced_read_path`] (programmatic
//!    `Database` config) wins, else
//! 2. the `CQLITE_READ_PATH` env var (`auto|point|full`, case-insensitive), read
//!    **once** into a process-global [`OnceLock`], else
//! 3. [`ReadPathMode::Auto`].
//!
//! An unrecognized env value is a loud [`Error::InvalidReadPath`], never a silent
//! fall-through to `Auto` (a typo'd knob that silently no-ops would defeat the
//! knob's purpose). Because the env read is cached once per process, the
//! per-query switch used by tests and by `Database`-level config is the
//! precedence-winning config field, not the env var.

use super::super::access_path::FallbackReason;
use super::PartitionLookupOutcome;
use crate::config::ReadPathMode;
use crate::{Error, Result};
use std::sync::OnceLock;

/// The environment variable that forces the SELECT access path.
const READ_PATH_ENV: &str = "CQLITE_READ_PATH";

/// Parse a `CQLITE_READ_PATH` value (case-insensitive). A pure function so the
/// parse/validation logic is unit-testable without touching the process
/// environment (mirrors `now_clock::now_from`, roborev #1853).
pub(super) fn parse_read_path_mode(raw: &str) -> Result<ReadPathMode> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "auto" => Ok(ReadPathMode::Auto),
        "point" => Ok(ReadPathMode::Point),
        "full" => Ok(ReadPathMode::Full),
        _ => Err(Error::invalid_read_path(raw)),
    }
}

/// Pure resolver: config over env over `Auto`. Fully unit-testable via explicit
/// argument passing — no process-global state.
///
/// A `Some` config override wins outright (config precedence), so a bad env value
/// alongside an explicit config override does not error; the env is validated
/// only when it is the actual source of the decision.
pub(super) fn resolve_mode(
    config_override: Option<ReadPathMode>,
    env_value: Option<&str>,
) -> Result<ReadPathMode> {
    if let Some(mode) = config_override {
        return Ok(mode);
    }
    match env_value {
        Some(raw) => parse_read_path_mode(raw),
        None => Ok(ReadPathMode::Auto),
    }
}

/// The raw `CQLITE_READ_PATH` value, read once per process into a `OnceLock`.
/// `None` means the variable is unset; a set-but-empty value round-trips as
/// `Some("")` and is rejected loudly by [`parse_read_path_mode`] when consulted.
fn cached_env() -> Option<&'static str> {
    static ENV: OnceLock<Option<String>> = OnceLock::new();
    ENV.get_or_init(|| std::env::var(READ_PATH_ENV).ok())
        .as_deref()
}

/// Resolve the effective [`ReadPathMode`] for a query: the executor's config
/// override (if any) wins, else the once-read `CQLITE_READ_PATH` env, else
/// `Auto`. Returns [`Error::InvalidReadPath`] when the env is the decision source
/// and holds an unrecognized value.
pub(super) fn resolve_read_path_mode(
    config_override: Option<ReadPathMode>,
) -> Result<ReadPathMode> {
    resolve_mode(config_override, cached_env())
}

/// What a call site should do with a classified outcome after the forcing gate.
#[derive(Debug)]
pub(super) enum ForcedPlan {
    /// Run the classifier's outcome unchanged (`Auto`, or `Point` with a targeted
    /// outcome the site can genuinely serve).
    Proceed(PartitionLookupOutcome),
    /// Force the full-scan + reconciliation path. The site must record
    /// [`FallbackReason::ForcedFullScan`] and run the same full-scan code `Auto`
    /// falls back to, so rows/order are identical to the organic fallback.
    ForceFullScan,
}

/// The single forcing gate over the classifier's outcome (issue #1918). Applied
/// at every executor call site through this one wrapper so no site re-implements
/// the forcing policy.
///
/// - `Auto` passes the outcome through unchanged.
/// - `Full` forces a full scan regardless of the outcome.
/// - `Point` fails closed on a classification `Fallback` up front; a targeted
///   outcome proceeds and is checked again post-call (see [`point_requires_engaged`])
///   because the actually-executed path (`engaged`) is only known after the
///   storage call.
pub(super) fn apply_forcing(
    outcome: PartitionLookupOutcome,
    mode: ReadPathMode,
) -> Result<ForcedPlan> {
    match mode {
        ReadPathMode::Auto => Ok(ForcedPlan::Proceed(outcome)),
        ReadPathMode::Full => Ok(ForcedPlan::ForceFullScan),
        ReadPathMode::Point => match outcome {
            PartitionLookupOutcome::Fallback(reason) => {
                Err(Error::forced_read_path_unavailable("point", reason.label()))
            }
            targeted => Ok(ForcedPlan::Proceed(targeted)),
        },
    }
}

/// Under `Point`, convert a post-call `engaged == false` (the storage surface did
/// not actually prune — e.g. the `tombstones` build's no-prune fallback) into the
/// distinct fail-closed error, naming the concrete reason. A no-op in every other
/// mode and whenever the path genuinely engaged.
pub(super) fn point_requires_engaged(
    mode: ReadPathMode,
    engaged: bool,
    reason: FallbackReason,
) -> Result<()> {
    if mode == ReadPathMode::Point && !engaged {
        return Err(Error::forced_read_path_unavailable("point", reason.label()));
    }
    Ok(())
}

/// Under `Point`, fail closed when a call site is about to run a NON-targeted
/// execution its surface cannot serve as a genuine partition lookup (e.g. a
/// WRITETIME/TTL metadata projection with no usable partition-key restriction,
/// which full-scans). A no-op in every other mode.
pub(super) fn point_forbids_fallback(mode: ReadPathMode, reason: FallbackReason) -> Result<()> {
    if mode == ReadPathMode::Point {
        return Err(Error::forced_read_path_unavailable("point", reason.label()));
    }
    Ok(())
}

/// The mirror image of [`point_forbids_fallback`]: under `Full`, fail closed when
/// the query qualifies ONLY for the specialized schema-less sole-pk targeted seek
/// (issue #1750). With no CQL schema the full-scan path's per-row predicate
/// backstop cannot reconstruct the partition-key column to match the literal, so a
/// forced full scan would silently return 0 rows instead of the row `auto`
/// returns — violating the spec's "identical to `auto`" SHALL. Rather than diverge
/// silently, fail loud with the same clear error. A no-op in every other mode and
/// whenever the query does not qualify for the schema-less seek.
pub(super) fn full_forbids_schemaless_seek(mode: ReadPathMode, seek_eligible: bool) -> Result<()> {
    if mode == ReadPathMode::Full && seek_eligible {
        return Err(Error::forced_read_path_unavailable(
            "full",
            "schema_less_sole_pk_lookup_requires_targeted_seek",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_is_case_insensitive_and_trims() {
        assert_eq!(parse_read_path_mode("auto").unwrap(), ReadPathMode::Auto);
        assert_eq!(parse_read_path_mode("POINT").unwrap(), ReadPathMode::Point);
        assert_eq!(parse_read_path_mode("  Full ").unwrap(), ReadPathMode::Full);
    }

    #[test]
    fn parse_rejects_unknown_value_loudly() {
        let err = parse_read_path_mode("compact").expect_err("unknown value must error");
        let msg = err.to_string();
        assert!(
            msg.contains("compact"),
            "error must name the invalid value: {msg}"
        );
        assert!(
            msg.contains("auto") && msg.contains("point") && msg.contains("full"),
            "error must name the allowed set: {msg}"
        );
        assert!(matches!(err, Error::InvalidReadPath { .. }));
    }

    #[test]
    fn resolve_unset_is_auto() {
        assert_eq!(resolve_mode(None, None).unwrap(), ReadPathMode::Auto);
    }

    #[test]
    fn resolve_reads_env_when_no_config() {
        assert_eq!(
            resolve_mode(None, Some("point")).unwrap(),
            ReadPathMode::Point
        );
        assert_eq!(
            resolve_mode(None, Some("full")).unwrap(),
            ReadPathMode::Full
        );
    }

    #[test]
    fn resolve_config_overrides_env() {
        // env says full, config says point -> config wins.
        assert_eq!(
            resolve_mode(Some(ReadPathMode::Point), Some("full")).unwrap(),
            ReadPathMode::Point
        );
        // A config override even bypasses an otherwise-invalid env value.
        assert_eq!(
            resolve_mode(Some(ReadPathMode::Auto), Some("bogus")).unwrap(),
            ReadPathMode::Auto
        );
    }

    #[test]
    fn resolve_invalid_env_without_config_errors() {
        let err = resolve_mode(None, Some("bogus")).expect_err("invalid env must error");
        assert!(matches!(err, Error::InvalidReadPath { .. }));
    }

    #[test]
    fn apply_forcing_auto_passes_through() {
        let plan = apply_forcing(
            PartitionLookupOutcome::Targeted(vec![1, 2, 3]),
            ReadPathMode::Auto,
        )
        .unwrap();
        assert!(matches!(
            plan,
            ForcedPlan::Proceed(PartitionLookupOutcome::Targeted(_))
        ));
    }

    #[test]
    fn apply_forcing_full_forces_full_scan_for_any_outcome() {
        for outcome in [
            PartitionLookupOutcome::Targeted(vec![9]),
            PartitionLookupOutcome::MultiTargeted(vec![vec![1], vec![2]]),
            PartitionLookupOutcome::Fallback(FallbackReason::NoSchema),
        ] {
            let plan = apply_forcing(outcome, ReadPathMode::Full).unwrap();
            assert!(matches!(plan, ForcedPlan::ForceFullScan));
        }
    }

    #[test]
    fn apply_forcing_point_errors_on_fallback() {
        let err = apply_forcing(
            PartitionLookupOutcome::Fallback(FallbackReason::PartitionKeyNotFullyConstrained),
            ReadPathMode::Point,
        )
        .expect_err("point on a classification fallback must fail closed");
        match err {
            Error::ForcedReadPathUnavailable { forced, reason } => {
                assert_eq!(forced, "point");
                assert_eq!(reason, "partition_key_not_fully_constrained");
            }
            other => panic!("expected ForcedReadPathUnavailable, got {other:?}"),
        }
    }

    #[test]
    fn apply_forcing_point_proceeds_on_targeted() {
        let plan = apply_forcing(
            PartitionLookupOutcome::Targeted(vec![7]),
            ReadPathMode::Point,
        )
        .unwrap();
        assert!(matches!(
            plan,
            ForcedPlan::Proceed(PartitionLookupOutcome::Targeted(_))
        ));
    }

    #[test]
    fn point_requires_engaged_errors_only_when_not_engaged_under_point() {
        // Point + not engaged -> error naming the reason.
        let err = point_requires_engaged(
            ReadPathMode::Point,
            false,
            FallbackReason::TombstonesBuildNoPrune,
        )
        .expect_err("point + !engaged must fail closed");
        match err {
            Error::ForcedReadPathUnavailable { reason, .. } => {
                assert_eq!(reason, "tombstones_build_no_prune")
            }
            other => panic!("expected ForcedReadPathUnavailable, got {other:?}"),
        }
        // Point + engaged -> ok. Other modes -> always ok.
        assert!(point_requires_engaged(
            ReadPathMode::Point,
            true,
            FallbackReason::TombstonesBuildNoPrune
        )
        .is_ok());
        assert!(point_requires_engaged(
            ReadPathMode::Auto,
            false,
            FallbackReason::TombstonesBuildNoPrune
        )
        .is_ok());
        assert!(point_requires_engaged(
            ReadPathMode::Full,
            false,
            FallbackReason::TombstonesBuildNoPrune
        )
        .is_ok());
    }

    #[test]
    fn point_forbids_fallback_errors_only_under_point() {
        assert!(
            point_forbids_fallback(ReadPathMode::Point, FallbackReason::MetadataScanPath).is_err()
        );
        assert!(
            point_forbids_fallback(ReadPathMode::Auto, FallbackReason::MetadataScanPath).is_ok()
        );
        assert!(
            point_forbids_fallback(ReadPathMode::Full, FallbackReason::MetadataScanPath).is_ok()
        );
    }

    #[test]
    fn full_forbids_schemaless_seek_errors_only_under_full_when_eligible() {
        // Only forced `full` on a seek-eligible query fails closed.
        let err = full_forbids_schemaless_seek(ReadPathMode::Full, true)
            .expect_err("forced full on a schema-less sole-pk seek must fail closed");
        match err {
            Error::ForcedReadPathUnavailable { forced, reason } => {
                assert_eq!(forced, "full");
                assert_eq!(reason, "schema_less_sole_pk_lookup_requires_targeted_seek");
            }
            other => panic!("expected ForcedReadPathUnavailable, got {other:?}"),
        }
        // Not eligible under full -> no error (falls through to the full scan).
        assert!(full_forbids_schemaless_seek(ReadPathMode::Full, false).is_ok());
        // Auto/point never trip this guard (they keep the specialized seek).
        assert!(full_forbids_schemaless_seek(ReadPathMode::Auto, true).is_ok());
        assert!(full_forbids_schemaless_seek(ReadPathMode::Point, true).is_ok());
    }
}
