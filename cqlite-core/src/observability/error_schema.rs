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
//! | `Corruption`   | `corruption`     | `Corruption`                                                       |
//! | `Schema`       | `schema`         | `Schema`, `Table`                                                  |
//! | `Parsing`      | `parsing`        | `Parse`, `CqlParse`, `InvalidFormat`, `UnsupportedFormat`         |
//! | `Storage`      | `storage`        | `Storage`, `Memory`, `Index`, `Compaction`, `WriteDirLocked`     |
//! | `Concurrency`  | `concurrency`    | `Concurrency`, `Transaction`                                       |
//! | `Constraints`  | `constraints`    | `ConstraintViolation`, `AlreadyExists`                            |
//! | `Query`        | `query`          | `QueryExecution`, `UnsupportedQuery`, `InvalidInput`             |
//! | `Other`        | `other`          | `Configuration`, `InvalidState`, `InvalidOperation`, `NotFound`,  |
//! |                |                  | `Internal`, `Wasm`, and any future variant (catch-all)            |
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
pub enum ErrorCategory {
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
    /// Everything else (configuration, internal, platform, catch-all).
    Other,
}

impl ErrorCategory {
    /// Stable, low-cardinality label. Safe to use as a metric/span attribute
    /// value — these strings never change for a given variant.
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorCategory::Io => "io",
            ErrorCategory::Serialization => "serialization",
            ErrorCategory::Corruption => "corruption",
            ErrorCategory::Schema => "schema",
            ErrorCategory::Parsing => "parsing",
            ErrorCategory::Storage => "storage",
            ErrorCategory::Concurrency => "concurrency",
            ErrorCategory::Constraints => "constraints",
            ErrorCategory::Query => "query",
            ErrorCategory::Other => "other",
        }
    }

    /// All variants, for tests and exhaustiveness checks.
    pub const ALL: &'static [ErrorCategory] = &[
        ErrorCategory::Io,
        ErrorCategory::Serialization,
        ErrorCategory::Corruption,
        ErrorCategory::Schema,
        ErrorCategory::Parsing,
        ErrorCategory::Storage,
        ErrorCategory::Concurrency,
        ErrorCategory::Constraints,
        ErrorCategory::Query,
        ErrorCategory::Other,
    ];
}

impl std::fmt::Display for ErrorCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Map a [`cqlite_core::Error`](crate::error::Error) to its telemetry
/// [`ErrorCategory`]. This is the single classification point; both
/// `Error::obs_category` and `record_error` route through it.
pub(crate) fn classify(err: &Error) -> ErrorCategory {
    match err {
        Error::Io(_) | Error::InvalidPath(_) | Error::Timeout(_) => ErrorCategory::Io,

        Error::Serialization { .. } | Error::TypeConversion(_) => ErrorCategory::Serialization,

        Error::Corruption(_) => ErrorCategory::Corruption,

        Error::Schema(_) | Error::Table(_) => ErrorCategory::Schema,

        Error::Parse(_)
        | Error::CqlParse(_)
        | Error::InvalidFormat(_)
        | Error::UnsupportedFormat(_)
        | Error::UnsupportedVersion { .. } => ErrorCategory::Parsing,

        Error::Storage(_)
        | Error::Memory(_)
        | Error::Index(_)
        | Error::Compaction(_)
        | Error::WriteDirLocked { .. } => ErrorCategory::Storage,

        Error::Concurrency(_) | Error::Transaction(_) => ErrorCategory::Concurrency,

        Error::ConstraintViolation(_) | Error::AlreadyExists(_) => ErrorCategory::Constraints,

        Error::QueryExecution(_)
        | Error::ResultTooLarge { .. }
        | Error::UnsupportedQuery(_)
        | Error::InvalidInput(_) => ErrorCategory::Query,

        // Catch-all for the remaining variants and any future additions. Listed
        // explicitly (no wildcard arm besides wasm) so that adding a new Error
        // variant forces a compile decision here.
        Error::Configuration(_)
        | Error::InvalidState(_)
        | Error::InvalidOperation(_)
        | Error::NotFound(_)
        | Error::Cancelled
        | Error::Internal(_) => ErrorCategory::Other,

        #[cfg(target_arch = "wasm32")]
        Error::Wasm(_) => ErrorCategory::Other,
    }
}

impl Error {
    /// Telemetry error category for this error (issue #1038).
    ///
    /// Distinct from [`Error::category`](crate::error::Error::category), which
    /// returns the developer-facing [`crate::error::ErrorCategory`]. This one
    /// returns the bounded, monitoring-oriented
    /// [`crate::observability::ErrorCategory`].
    pub fn obs_category(&self) -> ErrorCategory {
        classify(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;

    #[test]
    fn as_str_is_lowercase_and_unique() {
        let mut seen = std::collections::HashSet::new();
        for c in ErrorCategory::ALL {
            let s = c.as_str();
            assert_eq!(s, s.to_ascii_lowercase());
            assert!(seen.insert(s), "duplicate category label {s}");
        }
        assert_eq!(seen.len(), ErrorCategory::ALL.len());
    }

    #[test]
    fn display_matches_as_str() {
        for c in ErrorCategory::ALL {
            assert_eq!(c.to_string(), c.as_str());
        }
    }

    // Exhaustively cover every Error constructor / variant -> expected category.
    #[test]
    fn classify_every_error_variant() {
        use ErrorCategory::*;

        let io = std::io::Error::other("x");
        assert_eq!(Error::from(io).obs_category(), Io);
        assert_eq!(Error::invalid_path("p").obs_category(), Io);
        assert_eq!(Error::Timeout("t".into()).obs_category(), Io);

        assert_eq!(Error::serialization("s").obs_category(), Serialization);
        assert_eq!(Error::type_conversion("t").obs_category(), Serialization);

        assert_eq!(Error::corruption("c").obs_category(), Corruption);

        assert_eq!(Error::schema("s").obs_category(), Schema);
        assert_eq!(Error::Table("t".into()).obs_category(), Schema);

        assert_eq!(Error::parse("p").obs_category(), Parsing);
        assert_eq!(Error::cql_parse("p").obs_category(), Parsing);
        assert_eq!(Error::invalid_format("f").obs_category(), Parsing);
        assert_eq!(Error::unsupported_format("f").obs_category(), Parsing);

        assert_eq!(Error::storage("s").obs_category(), Storage);
        assert_eq!(Error::memory("m").obs_category(), Storage);
        assert_eq!(Error::index("i").obs_category(), Storage);
        assert_eq!(Error::compaction("c").obs_category(), Storage);
        assert_eq!(Error::write_dir_locked("/d").obs_category(), Storage);

        assert_eq!(Error::concurrency("c").obs_category(), Concurrency);
        assert_eq!(Error::transaction("t").obs_category(), Concurrency);

        assert_eq!(Error::constraint_violation("c").obs_category(), Constraints);
        assert_eq!(Error::already_exists("a").obs_category(), Constraints);

        assert_eq!(Error::query_execution("q").obs_category(), Query);
        assert_eq!(Error::unsupported_query("q").obs_category(), Query);
        assert_eq!(Error::invalid_input("i").obs_category(), Query);

        assert_eq!(Error::configuration("c").obs_category(), Other);
        assert_eq!(Error::invalid_state("s").obs_category(), Other);
        assert_eq!(Error::invalid_operation("o").obs_category(), Other);
        assert_eq!(Error::not_found("n").obs_category(), Other);
        assert_eq!(Error::internal("i").obs_category(), Other);
    }
}
