//! Integration tests for the CQL SELECT parser/optimizer/executor wiring.
//!
//! ## Issue #1880 — retirement of the write-path integration tests
//!
//! This module historically drove `db.execute("INSERT …")` to populate an
//! in-memory table and then asserted the row-count of `SELECT`s over that data.
//! The WAL/MemTable write path those tests depended on was deleted in Issue #175
//! (`execute` on an INSERT now returns `UnsupportedFormat`), so every insert-then-
//! query test could only ever panic. Because they were gated behind the
//! non-default `experimental` feature they were NOT exercised by the feature-scoped
//! gate lane and rotted silently until surfaced under `cargo test --all-features`.
//!
//! Those tests were **removed** rather than `#[ignore]`d (a plain ignore would
//! leave dead code referencing a deleted path). Row-based `SELECT` coverage now
//! lives in the real-SSTable integration/parity tests (`test-data/datasets`).
//!
//! What remains here are the two tests that never depended on the write path — a
//! pure parser assertion and an optimizer/executor wiring check. They are gated on
//! `state_machine` (a default feature) so they run in the normal gate lanes and can
//! no longer rot in an unexercised feature combo.

#[cfg(test)]
#[cfg(feature = "state_machine")]
mod tests {
    use crate::{
        query::{parse_select, SelectExecutor, SelectOptimizer, SelectStatement},
        schema::SchemaManager,
        storage::StorageEngine,
        types::TableId,
        Config,
    };
    use std::sync::Arc;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_parser_only() {
        // Test the parser without a database.
        // Cassandra 5 compliant CQL - aggregate functions only.
        let sql = "SELECT COUNT(*) FROM orders WHERE active = true";

        let statement = parse_select(sql).unwrap();

        assert!(statement.requires_aggregation());
        assert!(statement.group_by.is_none()); // No GROUP BY in simple aggregate
        assert!(statement.having_clause.is_none()); // No HAVING in simple aggregate
        assert!(statement.order_by.is_none()); // No ORDER BY in simple aggregate
        assert!(statement.limit.is_none()); // No LIMIT in simple aggregate
    }

    #[tokio::test]
    async fn test_optimizer_and_executor_integration() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(crate::platform::Platform::new(&config).await.unwrap());

        let storage = Arc::new(
            StorageEngine::open(
                temp_dir.path(),
                &config,
                platform,
                #[cfg(feature = "state_machine")]
                None,
            )
            .await
            .unwrap(),
        );
        let schema = Arc::new(SchemaManager::new(temp_dir.path()).await.unwrap());

        let optimizer = SelectOptimizer::new(schema.clone(), storage.clone());
        let _executor = SelectExecutor::new(schema.clone(), storage.clone());

        let statement = SelectStatement::select_all_from(TableId::new("users"));
        let optimized_plan = optimizer.optimize(statement).await.unwrap();
        assert!(!optimized_plan.execution_steps.is_empty());

        // The executor would run the plan, but we need actual SSTable files for that.
        // This test validates the integration works without runtime errors.
    }
}
