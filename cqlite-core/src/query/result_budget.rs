//! Byte-bounded result-budget enforcement (issue #1582 / D6, narrow subset).
//!
//! The modern materializing SELECT path ([`crate::query::select_executor`])
//! guards its FINAL result with a BYTE ceiling
//! ([`crate::config::QueryConfig::max_result_bytes`]) plus a secondary row-count
//! safety valve ([`crate::config::QueryConfig::max_result_rows`]). The check is
//! applied ONCE, on the fully-materialized result (after the whole step
//! pipeline), by [`crate::query::select_executor::SelectExecutor::execute`].
//!
//! SCOPE: the LEGACY [`QueryExecutor`](crate::query::executor::QueryExecutor)
//! path — non-SELECT statements and any cached legacy SELECT plan reused via the
//! plan-cache HIT branch — is NOT covered by this module's estimator directly;
//! `QueryEngine::enforce_legacy_result_budget` applies the same ceiling at those
//! sites. Since issue #1750 ad-hoc SELECTs (including simple `WHERE id = <value>`
//! point lookups) route through the modern `SelectExecutor` and are budgeted
//! there. (D6 redesign tracked on #1582.)
//!
//! This module is compiled unconditionally (unlike `select_executor`, which is
//! `state_machine`-gated).

use super::result::QueryRow;
use crate::{Error, Result};

/// Estimate the logical size, in bytes, of a materialized [`QueryRow`], reusing
/// the shared row-cache estimator ([`crate::memory::estimate_value_size`]). Sums
/// the per-value estimate over the row's column values, plus the retained
/// [`RowKey`](crate::types::RowKey) bytes (issue #1582): every materialized row
/// keeps the original partition/clustering key, so a query projecting small
/// non-key columns from rows with very large keys must still count the key bytes
/// against `max_result_bytes`. Uses saturating addition so a pathological row can
/// never overflow `usize`.
pub(crate) fn estimate_query_row_bytes(row: &QueryRow) -> usize {
    let values_bytes: usize = row
        .values
        .values()
        .map(crate::memory::estimate_value_size)
        .sum();
    values_bytes.saturating_add(row.key.as_bytes().len())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::estimate_value_size;
    use crate::query::result::{QueryRow, RowMetadata};
    use crate::types::{RowKey, Value};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn row_with(key_bytes: usize, value: Value) -> QueryRow {
        let mut values: HashMap<Arc<str>, Value> = HashMap::new();
        values.insert(Arc::from("c"), value);
        QueryRow {
            values,
            key: RowKey::new(vec![b'k'; key_bytes]),
            metadata: RowMetadata::default(),
            cell_metadata: None,
        }
    }

    /// Issue #1582: a wide partition key with a tiny projected value must count
    /// the retained key bytes against the budget. Value bytes alone stay under,
    /// so the budget only trips because the key is included.
    #[test]
    fn wide_key_small_value_trips_budget() {
        // Value::Integer estimates 4 bytes; key is 1000 bytes.
        let rows = vec![row_with(1000, Value::Integer(1))];
        let value_only = rows[0]
            .values
            .values()
            .map(estimate_value_size)
            .sum::<usize>();
        assert_eq!(value_only, 4, "projected value estimate should be small");

        // Budget sits above the value-only estimate but below value+key.
        let budget = 500;
        assert!(value_only <= budget, "keys-ignored estimate would NOT trip");

        let err = enforce_materialized_rows(&rows, budget, usize::MAX)
            .expect_err("wide key must push the row over the byte budget");
        match err {
            Error::ResultTooLarge {
                budget_bytes,
                estimated_bytes,
                rows: n,
            } => {
                assert_eq!(budget_bytes, budget);
                assert!(estimated_bytes > budget, "estimate must include key bytes");
                assert!(
                    estimated_bytes >= 1000,
                    "estimate must include the 1000-byte key"
                );
                assert_eq!(n, 1);
            }
            other => panic!("expected ResultTooLarge, got {other:?}"),
        }
    }

    /// Sanity: within-budget rows (key + value) still pass.
    #[test]
    fn small_key_small_value_passes() {
        let rows = vec![row_with(8, Value::Integer(1))];
        enforce_materialized_rows(&rows, 500, usize::MAX)
            .expect("small key + value must stay under budget");
    }
}
