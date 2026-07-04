//! Shared byte-bounded result-budget enforcement (issue #1582 / D6).
//!
//! The materializing SELECT path guards a result set with a BYTE ceiling
//! ([`crate::config::QueryConfig::max_result_bytes`]) plus a secondary row-count
//! safety valve ([`crate::config::QueryConfig::max_result_rows`]). Both the
//! modern optimizer path ([`crate::query::select_executor`]) and the legacy
//! [`QueryExecutor`](crate::query::executor::QueryExecutor) point-lookup path
//! (used for simple `WHERE id = <value>` lookups so key handling stays
//! consistent with INSERT) must apply the SAME guard, so the logic lives here in
//! one place rather than being duplicated (a divergence hazard).
//!
//! This module is compiled unconditionally (unlike `select_executor`, which is
//! `state_machine`-gated) so the legacy engine path can reach it in every build
//! configuration.

use super::result::QueryRow;
use crate::{Error, Result};

/// Estimate the logical size, in bytes, of a materialized [`QueryRow`], reusing
/// the shared row-cache estimator ([`crate::memory::estimate_value_size`]). Sums
/// the per-value estimate over the row's column values.
pub(crate) fn estimate_query_row_bytes(row: &QueryRow) -> usize {
    row.values
        .values()
        .map(crate::memory::estimate_value_size)
        .sum()
}

/// Sum [`estimate_query_row_bytes`] over a result set, saturating so a pathological
/// estimate can never overflow `usize` (it stays at `usize::MAX`, which still
/// trips the byte budget).
pub(crate) fn estimate_result_bytes(rows: &[QueryRow]) -> usize {
    rows.iter().fold(0usize, |acc, row| {
        acc.saturating_add(estimate_query_row_bytes(row))
    })
}

/// Enforce the byte-bounded result budget (primary) and the row-count safety
/// valve (secondary) on a materialized result set (issue #1582 / D6).
///
/// `result_bytes` is the logical-byte estimate of `results`. Returns
/// [`Error::ResultTooLarge`] (with a remedy message: add `LIMIT` or stream) when
/// the byte budget is exceeded, or the legacy query-execution error when the
/// row-count valve trips.
pub(crate) fn enforce_result_budget(
    results: &[QueryRow],
    result_bytes: usize,
    byte_budget: usize,
    max_rows: usize,
) -> Result<()> {
    if result_bytes > byte_budget {
        return Err(Error::ResultTooLarge {
            budget_bytes: byte_budget,
            estimated_bytes: result_bytes,
            rows: results.len(),
        });
    }
    if results.len() > max_rows {
        return Err(Error::query_execution(
            "Result set too large, consider adding LIMIT".to_string(),
        ));
    }
    Ok(())
}

/// Enforce the budget on a fully-materialized set of rows in one call: estimate
/// the total bytes, then apply [`enforce_result_budget`]. Used by the legacy
/// engine point-lookup path, which materializes the whole result before checking.
pub(crate) fn enforce_materialized_rows(
    rows: &[QueryRow],
    byte_budget: usize,
    max_rows: usize,
) -> Result<()> {
    let result_bytes = estimate_result_bytes(rows);
    enforce_result_budget(rows, result_bytes, byte_budget, max_rows)
}
