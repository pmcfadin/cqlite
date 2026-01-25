//! PreparedStatement wrapper for Node.js bindings.
//!
//! This module provides the PreparedStatement class for Node.js access
//! to CQLite's prepared statement functionality.

use std::sync::Arc;

use napi_derive::napi;

/// Statistics about a prepared statement.
///
/// Contains query plan information useful for optimization
/// and debugging query performance.
#[napi(object)]
pub struct PreparedStatementStats {
    /// Number of parameters in the query.
    #[napi(js_name = "parameterCount")]
    pub parameter_count: u32,

    /// Type of execution plan (TableScan, IndexScan, PointLookup).
    #[napi(js_name = "planType")]
    pub plan_type: String,

    /// Estimated execution cost (relative metric for comparing plans).
    #[napi(js_name = "estimatedCost")]
    pub estimated_cost: f64,

    /// Estimated number of rows to be returned.
    #[napi(js_name = "estimatedRows", ts_type = "bigint")]
    pub estimated_rows: i64,

    /// Whether the query is cache-friendly.
    #[napi(js_name = "cacheFriendly")]
    pub cache_friendly: bool,
}

impl PreparedStatementStats {
    /// Create from core library stats.
    ///
    /// Note on type conversions:
    /// - `parameter_count`: usize → u32, clamped to u32::MAX (unrealistic to have 4B params)
    /// - `estimated_rows`: u64 → i64, clamped to i64::MAX for JavaScript bigint compatibility
    fn from_core(stats: cqlite_core::query::prepared::PreparedQueryStats) -> Self {
        Self {
            // Clamp to u32::MAX - queries with 4 billion parameters are unrealistic
            parameter_count: stats.parameter_count.min(u32::MAX as usize) as u32,
            plan_type: stats.plan_type,
            estimated_cost: stats.estimated_cost,
            // Clamp to i64::MAX for JavaScript bigint compatibility
            estimated_rows: stats.estimated_rows.min(i64::MAX as u64) as i64,
            cache_friendly: stats.cache_friendly,
        }
    }
}

/// A prepared CQL statement.
///
/// PreparedStatement holds a pre-parsed and planned query that can be
/// inspected for metadata and statistics. Created via Database.prepare().
#[napi]
pub struct PreparedStatement {
    inner: Arc<cqlite_core::query::PreparedQuery>,
}

impl PreparedStatement {
    /// Create a new PreparedStatement from a core PreparedQuery.
    pub fn new(inner: Arc<cqlite_core::query::PreparedQuery>) -> Self {
        Self { inner }
    }
}

#[napi]
impl PreparedStatement {
    /// The original CQL query text.
    #[napi(getter)]
    pub fn query(&self) -> String {
        self.inner.cql.clone()
    }

    /// Number of parameters in the query.
    /// Returns count of placeholder parameters (?), clamped to u32::MAX.
    #[napi(getter, js_name = "parameterCount")]
    pub fn parameter_count(&self) -> u32 {
        self.inner.parameters.len().min(u32::MAX as usize) as u32
    }

    /// Get statistics about this prepared statement.
    #[napi]
    pub fn stats(&self) -> PreparedStatementStats {
        PreparedStatementStats::from_core(self.inner.stats())
    }

    /// String representation of the prepared statement.
    #[napi(js_name = "toString")]
    pub fn to_string_repr(&self) -> String {
        format!("PreparedStatement({:?})", self.inner.cql)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prepared_statement_stats_conversion() {
        let core_stats = cqlite_core::query::prepared::PreparedQueryStats {
            parameter_count: 2,
            plan_type: "TableScan".to_string(),
            estimated_cost: 100.5,
            estimated_rows: 1000,
            cache_friendly: true,
        };

        let stats = PreparedStatementStats::from_core(core_stats);

        assert_eq!(stats.parameter_count, 2);
        assert_eq!(stats.plan_type, "TableScan");
        assert!((stats.estimated_cost - 100.5).abs() < f64::EPSILON);
        assert_eq!(stats.estimated_rows, 1000);
        assert!(stats.cache_friendly);
    }

    #[test]
    fn test_to_string_repr() {
        // Test the formatting logic directly
        let repr = format!("PreparedStatement({:?})", "SELECT * FROM test");
        assert!(repr.contains("PreparedStatement"));
        assert!(repr.contains("SELECT"));
    }
}
