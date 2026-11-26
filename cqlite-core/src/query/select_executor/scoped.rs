//! Public `execute` entry: scopes the SELECT's needed columns around the whole query
//! (issue #4640, Q51) so the cross-generation merged read reassembles only the
//! collections the query reads. See `storage::read_projection`.

use crate::query::select_ast::{SelectClause, SelectStatement};
use std::collections::HashSet;
use std::sync::Arc;

use super::{OptimizedQueryPlan, QueryResult, Result, SelectExecutor};

impl SelectExecutor {
    /// Execute an optimized query plan (see `execute_inner` for the pipeline).
    pub async fn execute(&self, plan: OptimizedQueryPlan) -> Result<QueryResult> {
        let needed = needed_columns(&plan.statement);
        crate::storage::read_projection::with_needed(needed, self.execute_inner(plan)).await
    }
}

/// Columns a statement reads: projection, WHERE, GROUP BY, HAVING, ORDER BY and
/// WRITETIME/TTL arguments. `None` (every column) for `SELECT *`. Key columns are not
/// added here; the merged read always keeps them.
pub(super) fn needed_columns(stmt: &SelectStatement) -> Option<Arc<HashSet<String>>> {
    if matches!(stmt.select_clause, SelectClause::All) {
        return None;
    }
    Some(Arc::new(
        stmt.get_referenced_columns()
            .into_iter()
            .map(|c| c.column)
            .collect(),
    ))
}
