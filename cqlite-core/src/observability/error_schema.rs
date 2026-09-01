//! Low-cardinality error taxonomy for observability (issue #1038).
//!
//! This taxonomy is intentionally distinct from [`crate::error::ErrorCategory`]:
//! that one is a 14-variant developer-facing grouping consumed by the CLI and
//! existing tests, and changing it would break public API. The taxonomy here is
//! the *telemetry* taxonomy — a small, stable, bounded set of `&'static str`
//! labels safe to use as a metric/span attribute value (see
//! [`crate::observability::catalog::attr::ERROR_CATEGORY`]).
//!
//! # Taxonomy
//!
//! | Variant        | `as_str()`       | Maps from `cqlite_core::Error` …                                  |
//! |----------------|------------------|-------------------------------------------------------------------|
//! | `Io`           | `io`             | `Io`, `InvalidPath`, `Timeout`                                     |
//! | `Serialization`| `serialization`  | `Serialization`, `TypeConversion`                                 |
//! | `Corruption`   | `corruption`     | `Corruption`, `CorruptCommitLogFrame`, `ColumnDecode`             |
//! | `Schema`       | `schema`         | `Schema`, `Table`                                                  |
//! | `Parsing`      | `parsing`        | `Parse`, `CqlParse`, `InvalidFormat`, `UnsupportedFormat`,        |
//! |                |                  | `UnsupportedVersion`, `UnsupportedCommitLogVersion`               |
//! | `Storage`      | `storage`        | `Storage`, `Memory`, `Index`, `Compaction`, `WriteDirLocked`     |
//! | `Concurrency`  | `concurrency`    | `Concurrency`, `Transaction`                                       |
//! | `Constraints`  | `constraints`    | `ConstraintViolation`, `AlreadyExists`                            |
//! | `Query`        | `query`          | `QueryExecution`, `UnsupportedQuery`, `InvalidInput`,             |
//! |                |                  | `ResultTooLarge`, `ForcedReadPathUnavailable`, `InvalidReadPath`  |
//! | `Cancelled`    | `cancelled`      | `Cancelled` (issue #2264 — a cooperative abort, never `Io`)       |
//! | `Timeout`      | `timeout`        | `QueryTimeout` (issue #1695 — an operator budget, never data)     |
//! | `Other`        | `other`          | `Configuration`, `InvalidState`, `InvalidOperation`, `NotFound`,  |
//! |                |                  | `Internal`, `Wasm` (`wasm32` builds only)                         |
//!
//! The table is EXACT, not illustrative: every row's `Maps from` column lists the
//! COMPLETE set of `Error` variants [`classify`] routes to that category, `Other`
//! included. There is **no catch-all**. [`classify`] matches on `&Error` with every
//! arm an explicit `Error::<Variant>` pattern (pinned by
//! `error_schema_tests::classify_has_no_catch_all_arm`), so a newly-added `Error`
//! variant is a COMPILE ERROR until it is categorised by hand — it is never
//! silently absorbed into `Other`. `Wasm` is `#[cfg(target_arch = "wasm32")]`-gated
//! and therefore exists only in `wasm32` builds; it is listed because the table
//! describes the enum, not one target.
//!
//! `error_schema_tests::every_error_variant_classify_routes_is_documented_in_the_taxonomy_table`
//! enforces variant→category set equality against [`classify`]'s match arms in
//! both directions (issue #1705, AI5 of epic #1686): a variant routed but
//! undocumented, a documented variant that is never routed, and a variant listed
//! under the wrong category all fail. The `Maps from` column is parsed
//! fail-closed — a non-parenthetical item that is not a backticked variant name,
//! or a parenthetical that claims catch-all behaviour, reds the guard rather than
//! being silently dropped as prose (which is how the stale "any future variant
//! (catch-all)" claim survived here).
//!
//! **Scope: telemetry only.** The language bindings do NOT derive from
//! [`classify`]: `cqlite_ffi_common::error_contract` (issue #1451) mirrors the distinct
//! [`Error::category`](crate::error::Error::category) enum, and nothing pins the
//! two together — `QueryTimeout` is `Timeout` here and `Query` there.
//!
//! # Relation to spans and CLI exit codes
//!
//! - **Spans**: [`crate::observability::record_error`] attaches the
//!   `as_str()` value as the `cqlite.error.category` attribute on the
//!   `cqlite.errors.total` counter and as a span event field, and marks the
//!   current span `otel.status_code = ERROR`.
//! - **CLI exit codes** (`cqlite-cli/src/error.rs`): the CLI maps the *raw*
//!   `Error` variants to numeric exit codes (2/3/4/5/6). This taxonomy is a
//!   coarser, monitoring-oriented view; the rough correspondence is:
//!   `Schema → exit 3`, `Io`/`Storage` → exit 4, `Query`/`Parsing` → exit 5.
//!   The two are deliberately decoupled: exit codes are an operator contract,
//!   the telemetry taxonomy is tuned for dashboards/alerts.

use crate::error::Error;

/// Bounded, telemetry-safe error categories. The total count is small and
/// fixed, making `as_str()` values safe as metric/span attribute values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ObsErrorCategory {
    /// Filesystem / OS I/O, paths, timeouts.
    Io,
    /// (De)serialization and type-conversion failures.
    Serialization,
    /// On-disk data corruption / checksum failures.
    Corruption,
    /// Schema / catalog problems.
    Schema,
    /// Binary-format / CQL text parsing failures.
    Parsing,
    /// Storage-engine, memory, index, compaction, locking.
    Storage,
    /// Concurrency / transaction failures.
    Concurrency,
    /// Constraint violations and conflicts.
    Constraints,
    /// Query execution / unsupported queries / bad input.
    Query,
    /// A cooperative cancellation / abort (issue #2264). Kept distinct from
    /// `Other` (and never `Io`) so dashboards can see cancellation rate
    /// separately from genuine errors — a cancelled `do_get` is an expected
    /// outcome, not a fault.
    Cancelled,
    /// A query exceeded its configured execution budget
    /// (`query.max_execution_time`, issue #1695). Its OWN bucket, never
    /// `Corruption` (an operator-imposed deadline is not damaged data) and never
    /// the generic `Other` bucket (a rising timeout rate is the signal that the budget
    /// is too tight or a scan has regressed — the one thing dashboards must see).
    Timeout,
    /// Everything else: configuration, invalid state/operation, not-found,
    /// internal, platform. NOT a catch-all — [`classify`] names every `Error`
    /// variant explicitly, so a new variant lands here only when a human puts it
    /// here (see the module-doc taxonomy table).
    Other,
}

impl ObsErrorCategory {
    /// Stable, low-cardinality label. Safe to use as a metric/span attribute
    /// value — these strings never change for a given variant.
    pub fn as_str(self) -> &'static str {
        match self {
            ObsErrorCategory::Io => "io",
            ObsErrorCategory::Serialization => "serialization",
            ObsErrorCategory::Corruption => "corruption",
            ObsErrorCategory::Schema => "schema",
            ObsErrorCategory::Parsing => "parsing",
            ObsErrorCategory::Storage => "storage",
            ObsErrorCategory::Concurrency => "concurrency",
            ObsErrorCategory::Constraints => "constraints",
            ObsErrorCategory::Query => "query",
            ObsErrorCategory::Cancelled => "cancelled",
            ObsErrorCategory::Timeout => "timeout",
            ObsErrorCategory::Other => "other",
        }
    }

    /// All variants, for tests and exhaustiveness checks.
    pub const ALL: &'static [ObsErrorCategory] = &[
        ObsErrorCategory::Io,
        ObsErrorCategory::Serialization,
        ObsErrorCategory::Corruption,
        ObsErrorCategory::Schema,
        ObsErrorCategory::Parsing,
        ObsErrorCategory::Storage,
        ObsErrorCategory::Concurrency,
        ObsErrorCategory::Constraints,
        ObsErrorCategory::Query,
        ObsErrorCategory::Cancelled,
        ObsErrorCategory::Timeout,
        ObsErrorCategory::Other,
    ];
}

impl std::fmt::Display for ObsErrorCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Map a [`cqlite_core::Error`](crate::error::Error) to its telemetry
/// [`ObsErrorCategory`]. This is the single classification point; both
/// `Error::obs_category` and `record_error` route through it.
pub(crate) fn classify(err: &Error) -> ObsErrorCategory {
    match err {
        Error::Io(_) | Error::InvalidPath(_) | Error::Timeout(_) => ObsErrorCategory::Io,

        Error::Serialization { .. } | Error::TypeConversion(_) => ObsErrorCategory::Serialization,

        // Issue #3721: a per-column decode failure IS damaged/undecodable data at
        // the cell level — the same operator signal as `Corruption`, and never the
        // `Other` bucket, so a dashboard shows a read that failed on bad bytes.
        Error::Corruption(_)
        | Error::CorruptCommitLogFrame(_)
        | Error::ColumnDecode { .. } => ObsErrorCategory::Corruption,

        Error::Schema(_) | Error::Table(_) => ObsErrorCategory::Schema,

        Error::Parse(_)
        | Error::CqlParse(_)
        | Error::InvalidFormat(_)
        | Error::UnsupportedFormat(_)
        | Error::UnsupportedVersion { .. }
        | Error::UnsupportedCommitLogVersion { .. } => ObsErrorCategory::Parsing,

        Error::Storage(_)
        | Error::Memory(_)
        | Error::Index(_)
        | Error::Compaction(_)
        | Error::WriteDirLocked { .. } => ObsErrorCategory::Storage,

        Error::Concurrency(_) | Error::Transaction(_) => ObsErrorCategory::Concurrency,

        Error::ConstraintViolation(_) | Error::AlreadyExists(_) => ObsErrorCategory::Constraints,

        Error::QueryExecution(_)
        | Error::ResultTooLarge { .. }
        | Error::UnsupportedQuery(_)
        // Issue #1918: the read-path forcing knob failing closed is a query-time
        // outcome (`point` unavailable / invalid knob value).
        | Error::ForcedReadPathUnavailable { .. }
        | Error::InvalidReadPath { .. }
        | Error::InvalidInput(_) => ObsErrorCategory::Query,

        // Issue #2264: a cooperative cancellation is an expected outcome, not a
        // fault — kept out of both `Io` and the generic `Other` bucket.
        Error::Cancelled => ObsErrorCategory::Cancelled,

        // Issue #1695: an elapsed `query.max_execution_time` budget. Its own
        // bucket so it is never indistinguishable from `Corruption` on a
        // dashboard, and never buried in `Other`.
        Error::QueryTimeout { .. } => ObsErrorCategory::Timeout,

        // The remaining variants, each named EXPLICITLY. This is not a catch-all
        // and there is no wildcard arm anywhere in this match, so a newly-added
        // `Error` variant fails to compile until it is categorised here by hand
        // (pinned by `error_schema_tests::classify_has_no_catch_all_arm`).
        Error::Configuration(_)
        | Error::InvalidState(_)
        | Error::InvalidOperation(_)
        | Error::NotFound(_)
        | Error::Internal(_) => ObsErrorCategory::Other,

        #[cfg(target_arch = "wasm32")]
        Error::Wasm(_) => ObsErrorCategory::Other,
    }
}

impl Error {
    /// Telemetry error category for this error (issue #1038).
    ///
    /// Distinct from [`Error::category`](crate::error::Error::category), which
    /// returns the developer-facing [`crate::error::ErrorCategory`]. This one
    /// returns the bounded, monitoring-oriented
    /// [`crate::observability::ObsErrorCategory`].
    pub fn obs_category(&self) -> ObsErrorCategory {
        classify(self)
    }
}

/// Invariant + code↔doc completeness tests live in a sibling file so this file
/// stays pure source inside the campsite-rule target (#1116); they are logically
/// the `tests` submodule of this module.
#[cfg(test)]
#[path = "error_schema_tests.rs"]
mod tests;
