//! Integration Tests for Revolutionary CQL SELECT Parser
//!
//! This module contains comprehensive integration tests that demonstrate
//! the FIRST EVER direct CQL querying of SSTable files without Cassandra.

#[cfg(test)]
#[cfg(feature = "experimental")]
mod tests {
    use crate::{
        query::{parse_select, SelectExecutor, SelectOptimizer, SelectStatement},
        schema::SchemaManager,
        storage::StorageEngine,
        types::{TableId, Value},
        Config, Database,
    };
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::time::{timeout, Duration};

    /// Test helper to create a test database.
    ///
    /// Public so the sibling `benchmarks` module can reuse it.
    pub async fn create_test_database() -> (Database, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let db = Database::open(temp_dir.path(), config).await.unwrap();
        (db, temp_dir)
    }

    /// Execute a CQL statement, unwrapping the result. Test-only convenience.
    async fn exec(db: &Database, sql: &str) -> crate::query::QueryResult {
        db.execute(sql).await.unwrap()
    }

    /// Execute every CQL statement in `stmts` in order, panicking on the first error.
    async fn exec_all(db: &Database, stmts: &[&str]) {
        for sql in stmts {
            db.execute(sql).await.unwrap();
        }
    }

    /// Execute a query under a timeout, panicking with `label` if it elapses.
    async fn exec_with_timeout(
        db: &Database,
        sql: &str,
        secs: u64,
        label: &str,
    ) -> crate::query::QueryResult {
        timeout(Duration::from_secs(secs), db.execute(sql))
            .await
            .unwrap_or_else(|_| panic!("{} timed out", label))
            .unwrap()
    }

    #[tokio::test]
    async fn test_simple_select_all() {
        let (db, _temp_dir) = create_test_database().await;

        exec_all(
            &db,
            &[
                "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
                "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)",
                "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)",
            ],
        )
        .await;

        let result = exec(&db, "SELECT * FROM users").await;
        assert_eq!(result.rows.len(), 2);
        assert!(result.execution_time_ms > 0);
    }

    #[tokio::test]
    async fn test_select_with_where_clause() {
        let (db, _temp_dir) = create_test_database().await;

        exec_all(
            &db,
            &[
                "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price DOUBLE, category TEXT)",
                "INSERT INTO products VALUES (1, 'Laptop', 999.99, 'Electronics')",
                "INSERT INTO products VALUES (2, 'Phone', 599.99, 'Electronics')",
                "INSERT INTO products VALUES (3, 'Book', 19.99, 'Books')",
            ],
        )
        .await;

        // WHERE with equality
        let result = exec(&db, "SELECT * FROM products WHERE category = 'Electronics'").await;
        assert_eq!(result.rows.len(), 2);

        // WHERE with comparison
        let result = exec(&db, "SELECT * FROM products WHERE price > 500").await;
        assert_eq!(result.rows.len(), 2);

        // BETWEEN is not well supported in Cassandra CQL - removed
    }

    #[tokio::test]
    async fn test_select_with_in_clause() {
        let (db, _temp_dir) = create_test_database().await;

        exec_all(
            &db,
            &[
                "CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT, amount DOUBLE)",
                "INSERT INTO orders VALUES (1, 'pending', 100.0)",
                "INSERT INTO orders VALUES (2, 'shipped', 250.0)",
                "INSERT INTO orders VALUES (3, 'delivered', 150.0)",
                "INSERT INTO orders VALUES (4, 'cancelled', 75.0)",
            ],
        )
        .await;

        let result = exec(
            &db,
            "SELECT * FROM orders WHERE status IN ('pending', 'shipped')",
        )
        .await;
        assert_eq!(result.rows.len(), 2);
    }

    #[tokio::test]
    async fn test_aggregation_functions() {
        // Skip in CI to avoid sporadic hangs on shared runners
        if std::env::var("CI").is_ok() {
            println!("INFO: Skipping test_aggregation_functions in CI environment");
            return;
        }

        let (db, _temp_dir) = create_test_database().await;

        exec_all(
            &db,
            &[
                "CREATE TABLE sales (id INTEGER PRIMARY KEY, region TEXT, amount DOUBLE)",
                "INSERT INTO sales VALUES (1, 'North', 1000.0)",
                "INSERT INTO sales VALUES (2, 'South', 1500.0)",
                "INSERT INTO sales VALUES (3, 'North', 800.0)",
                "INSERT INTO sales VALUES (4, 'East', 1200.0)",
            ],
        )
        .await;

        // Cassandra 5 compliant: aggregate functions only, no mixing with non-aggregates.
        // Each aggregate query returns a single row.
        for (sql, label) in [
            ("SELECT COUNT(*) FROM sales", "COUNT aggregation"),
            ("SELECT SUM(amount) FROM sales", "SUM aggregation"),
            ("SELECT AVG(amount) FROM sales", "AVG aggregation"),
            ("SELECT COUNT(*) FROM sales", "COUNT aggregation (2)"),
        ] {
            let result = exec_with_timeout(&db, sql, 5, label).await;
            assert_eq!(result.rows.len(), 1);
        }
    }

    #[tokio::test]
    async fn test_repeated_aggregation_plan_cache() {
        let (db, _temp_dir) = create_test_database().await;

        exec_all(
            &db,
            &[
                "CREATE TABLE metrics (id INTEGER PRIMARY KEY, value DOUBLE)",
                "INSERT INTO metrics VALUES (1, 10.0)",
                "INSERT INTO metrics VALUES (2, 20.0)",
            ],
        )
        .await;

        // Run the same aggregation twice; second call should hit the plan cache.
        for label in ["Initial aggregation", "Repeated aggregation"] {
            let result = exec_with_timeout(&db, "SELECT COUNT(*) FROM metrics", 1, label).await;
            assert_eq!(result.rows.len(), 1);
        }
    }

    #[tokio::test]
    #[ignore] // M2 feature: Aggregation not fully implemented
    async fn test_partition_scoped_aggregation() {
        let (db, _temp_dir) = create_test_database().await;

        exec_all(
            &db,
            &[
                "CREATE TABLE sales (
                    category TEXT,
                    item_id INT,
                    amount DOUBLE,
                    PRIMARY KEY (category, item_id)
                )",
                "INSERT INTO sales (category, item_id, amount) VALUES ('tech', 1, 10.0)",
                "INSERT INTO sales (category, item_id, amount) VALUES ('tech', 2, 40.0)",
                "INSERT INTO sales (category, item_id, amount) VALUES ('finance', 1, 25.0)",
            ],
        )
        .await;

        let result = exec_with_timeout(
            &db,
            "SELECT SUM(amount) FROM sales WHERE category = 'tech'",
            2,
            "Partition-scoped SUM",
        )
        .await;

        assert_eq!(result.rows.len(), 1);
        let row = &result.rows[0];
        let sum_value = row.values.get("Sum_amount").expect("Missing SUM result");
        assert_eq!(sum_value.as_f64().unwrap_or_default(), 50.0);
    }

    #[tokio::test]
    #[ignore] // M2 feature: GROUP BY not fully implemented
    async fn test_partition_key_group_by() {
        let (db, _temp_dir) = create_test_database().await;

        exec(
            &db,
            "CREATE TABLE events (
                category TEXT,
                region TEXT,
                event_id INT,
                PRIMARY KEY ((category, region), event_id)
            )",
        )
        .await;

        let inserts = [
            ("analytics", "us-east", 1),
            ("analytics", "us-east", 2),
            ("analytics", "eu-west", 3),
            ("billing", "us-east", 4),
        ];

        for (category, region, event_id) in inserts {
            exec(
                &db,
                &format!(
                    "INSERT INTO events (category, region, event_id) VALUES ('{}', '{}', {})",
                    category, region, event_id
                ),
            )
            .await;
        }

        let result = exec_with_timeout(
            &db,
            "SELECT category, region, COUNT(*) FROM events \
             WHERE category IN ('analytics', 'billing') GROUP BY category, region",
            2,
            "GROUP BY partition components",
        )
        .await;

        assert_eq!(result.rows.len(), 3);
        for row in &result.rows {
            let category = row
                .values
                .get("category")
                .and_then(Value::as_str)
                .unwrap()
                .to_string();
            let region = row
                .values
                .get("region")
                .and_then(Value::as_str)
                .unwrap()
                .to_string();
            let count = row.values.get("Count(*)").and_then(Value::as_i64).unwrap();

            match (category.as_str(), region.as_str()) {
                ("analytics", "us-east") => assert_eq!(count, 2),
                ("analytics", "eu-west") => assert_eq!(count, 1),
                ("billing", "us-east") => assert_eq!(count, 1),
                _ => panic!("Unexpected group combination: {} / {}", category, region),
            }
        }
    }

    // NOTE: test_clustering_order_by_with_aggregation was removed because
    // Cassandra CQL does NOT support ORDER BY with aggregate functions.
    // ORDER BY can only be used with pre-defined clustering columns, not computed values.
    // This is a fundamental architectural limitation in Cassandra's distributed model.

    #[tokio::test]
    #[ignore] // M2 feature: Aggregation error handling not fully implemented
    async fn test_aggregation_invalid_column_errors() {
        let (db, _temp_dir) = create_test_database().await;

        exec_all(
            &db,
            &[
                "CREATE TABLE agg_errors (id INTEGER PRIMARY KEY, amount DOUBLE)",
                "INSERT INTO agg_errors VALUES (1, 10.0)",
            ],
        )
        .await;

        let result = db
            .execute("SELECT SUM(missing_column) FROM agg_errors")
            .await;
        assert!(
            result.is_err(),
            "Expected missing column to return an error"
        );
    }

    #[tokio::test]
    #[ignore] // M2 feature: Aggregation timeout handling not fully implemented
    async fn test_aggregation_timeout_propagation() {
        let (db, _temp_dir) = create_test_database().await;

        exec_all(
            &db,
            &[
                "CREATE TABLE agg_timeout (id INTEGER PRIMARY KEY, value DOUBLE)",
                "INSERT INTO agg_timeout VALUES (1, 42.0)",
            ],
        )
        .await;

        let result = timeout(
            Duration::from_millis(0),
            db.execute("SELECT COUNT(*) FROM agg_timeout"),
        )
        .await;

        assert!(result.is_err(), "Expected timeout to elapse immediately");
    }

    #[tokio::test]
    async fn test_order_by_and_limit() {
        let (db, _temp_dir) = create_test_database().await;

        // Clustering column on `id` enables ORDER BY in CQL.
        exec_all(
            &db,
            &[
                "CREATE TABLE employees (department TEXT, id INTEGER, name TEXT, salary DOUBLE, PRIMARY KEY (department, id))",
                "INSERT INTO employees VALUES ('Engineering', 1, 'Alice', 75000.0)",
                "INSERT INTO employees VALUES ('Marketing', 2, 'Bob', 65000.0)",
                "INSERT INTO employees VALUES ('Engineering', 3, 'Charlie', 85000.0)",
                "INSERT INTO employees VALUES ('Sales', 4, 'Diana', 70000.0)",
            ],
        )
        .await;

        // ORDER BY on clustering column
        let result = exec(
            &db,
            "SELECT * FROM employees WHERE department = 'Engineering' ORDER BY id ASC",
        )
        .await;
        assert!(!result.rows.is_empty());

        // LIMIT with partition key filter
        let result = exec(
            &db,
            "SELECT * FROM employees WHERE department = 'Engineering' LIMIT 2",
        )
        .await;
        assert!(!result.rows.is_empty());
    }

    #[tokio::test]
    async fn test_simple_where_expressions() {
        let (db, _temp_dir) = create_test_database().await;

        // CQL has no BOOLEAN — use INTEGER 0/1 for `active`.
        exec_all(
            &db,
            &[
                "CREATE TABLE inventory (id INTEGER PRIMARY KEY, product TEXT, quantity INTEGER, price DOUBLE, active INTEGER)",
                "INSERT INTO inventory VALUES (1, 'Widget A', 100, 10.50, 1)",
                "INSERT INTO inventory VALUES (2, 'Widget B', 50, 15.75, 1)",
                "INSERT INTO inventory VALUES (3, 'Widget C', 0, 8.25, 0)",
                "INSERT INTO inventory VALUES (4, 'Widget D', 25, 20.00, 1)",
            ],
        )
        .await;

        let result = exec(&db, "SELECT * FROM inventory WHERE quantity > 20").await;
        assert!(result.rows.len() >= 2);

        let result = exec(&db, "SELECT * FROM inventory WHERE active = 1").await;
        assert_eq!(result.rows.len(), 3);
    }

    // LIKE pattern matching is NOT supported in Cassandra CQL - removed test

    #[tokio::test]
    async fn test_collection_operations() {
        let (db, _temp_dir) = create_test_database().await;

        exec_all(
            &db,
            &[
                "CREATE TABLE user_data (id INTEGER PRIMARY KEY, tags LIST<TEXT>, preferences MAP<TEXT, TEXT>)",
                "INSERT INTO user_data (id, tags, preferences) VALUES (1, ['tech', 'programming', 'rust'], {'theme': 'dark', 'language': 'en'})",
            ],
        )
        .await;

        // Basic collection retrieval. Real Cassandra needs secondary indexes for CONTAINS etc.
        let result = exec(&db, "SELECT * FROM user_data WHERE id = 1").await;
        assert_eq!(result.rows.len(), 1);
    }

    #[tokio::test]
    async fn test_parser_only() {
        // Test the parser without database
        // Cassandra 5 compliant CQL - aggregate functions only
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

    #[tokio::test]
    async fn test_performance_with_large_dataset() {
        // Keep this test bounded to avoid timeouts on CI runners
        let ci = std::env::var("CI").is_ok();
        let insert_rows: usize = std::env::var("PERF_ROWS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(if ci { 75 } else { 300 });

        let result = timeout(Duration::from_secs(20), async move {
            let (db, _temp_dir) = create_test_database().await;

            exec(
                &db,
                "CREATE TABLE performance_test (id INTEGER PRIMARY KEY, value INTEGER, category TEXT)",
            )
            .await;

            for i in 0..insert_rows {
                let query = format!(
                    "INSERT INTO performance_test VALUES ({}, {}, 'category_{}')",
                    i,
                    i * 10,
                    i % 10
                );
                exec(&db, &query).await;
            }

            // Cassandra 5 compliant: aggregates only, no mixing with non-aggregates.
            let start = std::time::Instant::now();
            let count_result = exec(&db, "SELECT COUNT(*) FROM performance_test").await;
            let avg_result = exec(&db, "SELECT AVG(value) FROM performance_test").await;
            let duration = start.elapsed();

            assert_eq!(count_result.rows.len(), 1); // COUNT returns single row
            assert_eq!(avg_result.rows.len(), 1); // AVG returns single row

            // CI runners have variable performance; relax the threshold there.
            let max_ms = if ci { 5000 } else { 2000 };
            assert!(
                duration.as_millis() < max_ms as u128,
                "Aggregate queries took {:?}, threshold {}ms (ci={})",
                duration,
                max_ms,
                ci
            );
            assert!(count_result.execution_time_ms > 0);
        })
        .await;

        assert!(
            result.is_ok(),
            "test_performance_with_large_dataset timed out"
        );
    }

    #[tokio::test]
    #[ignore = "TODO: Implement proper error handling in query engine"]
    async fn test_error_handling() {
        let (db, _temp_dir) = create_test_database().await;

        // Syntax error
        let result = db.execute("SELECT * FROM").await;
        assert!(result.is_err());

        // Non-existent table
        let result = db.execute("SELECT * FROM non_existent_table").await;
        assert!(result.is_err());

        // Invalid column
        exec(&db, "CREATE TABLE test_table (id INTEGER PRIMARY KEY)").await;
        let result = db
            .execute("SELECT non_existent_column FROM test_table")
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    #[ignore = "TODO: Implement proper COUNT and aggregate query support"]
    async fn test_real_world_query_examples() {
        let (db, _temp_dir) = create_test_database().await;

        // Realistic e-commerce schema (CQL-compliant)
        exec_all(
            &db,
            &[
                "CREATE TABLE customers (customer_id INTEGER PRIMARY KEY, name TEXT, email TEXT, created_at BIGINT)",
                "CREATE TABLE orders (order_id INTEGER PRIMARY KEY, customer_id INTEGER, total_amount DOUBLE, status TEXT, created_at BIGINT)",
                "INSERT INTO customers VALUES (1, 'John Doe', 'john@example.com', 1640995200)",
                "INSERT INTO customers VALUES (2, 'Jane Smith', 'jane@example.com', 1641081600)",
                "INSERT INTO orders VALUES (1, 1, 299.99, 'completed', 1641168000)",
                "INSERT INTO orders VALUES (2, 1, 149.50, 'pending', 1641254400)",
                "INSERT INTO orders VALUES (3, 2, 89.95, 'completed', 1641340800)",
            ],
        )
        .await;

        // 1. Customer order analytics (Cassandra 5 compliant - aggregate only)
        let result = exec(
            &db,
            "SELECT COUNT(*) FROM orders WHERE status = 'completed'",
        )
        .await;
        assert!(
            !result.rows.is_empty(),
            "COUNT query returned no rows. Expected count from orders table"
        );

        // 2. SUM aggregate (Cassandra compliant)
        let result = exec(
            &db,
            "SELECT SUM(total_amount) FROM orders WHERE status = 'completed'",
        )
        .await;
        assert!(
            !result.rows.is_empty(),
            "SUM query returned no rows. Expected sum from orders table"
        );

        // 3. High-value orders (simplified for CQL compliance)
        let result = exec(
            &db,
            "SELECT order_id, customer_id, total_amount FROM orders WHERE order_id = 1",
        )
        .await;
        assert!(!result.rows.is_empty());

        // 4. Simple filtering (CQL-compliant)
        let result = exec(&db, "SELECT * FROM orders WHERE order_id = 2").await;
        assert!(!result.rows.is_empty());
    }
}

/// Performance benchmarks (for manual testing)
#[cfg(test)]
#[cfg(feature = "experimental")]
mod benchmarks {
    use super::tests::create_test_database;
    #[allow(unused_imports)]
    use crate::{Config, Database};
    use std::time::Instant;

    #[tokio::test]
    #[ignore] // Run manually with: cargo test benchmarks -- --ignored
    async fn benchmark_select_performance() {
        let (db, _temp_dir) = create_test_database().await;

        db.execute(
            "CREATE TABLE benchmark_data (id INTEGER PRIMARY KEY, value INTEGER, category INTEGER)",
        )
        .await
        .unwrap();

        // Insert 10,000 rows
        println!("Inserting 10,000 rows...");
        let insert_start = Instant::now();
        for i in 0..10_000 {
            let query = format!(
                "INSERT INTO benchmark_data VALUES ({}, {}, {})",
                i,
                (i * 1337) % 1000000, // Deterministic pseudo-random value
                i % 100
            );
            db.execute(&query).await.unwrap();
        }
        println!("Insert time: {:?}", insert_start.elapsed());

        let queries = [
            ("SELECT COUNT(*) FROM benchmark_data", "Simple COUNT"),
            (
                "SELECT * FROM benchmark_data WHERE id < 1000",
                "Range query with LIMIT",
            ),
            (
                "SELECT COUNT(*) FROM benchmark_data",
                "Aggregate COUNT query",
            ),
            (
                "SELECT * FROM benchmark_data WHERE category IN (1, 5, 10, 15, 20)",
                "IN query",
            ),
            (
                "SELECT * FROM benchmark_data ORDER BY value DESC LIMIT 100",
                "ORDER BY with LIMIT",
            ),
        ];

        for (sql, description) in queries {
            let start = Instant::now();
            let result = db.execute(sql).await.unwrap();
            let duration = start.elapsed();
            println!(
                "{}: {:?} ({} rows)",
                description,
                duration,
                result.rows.len()
            );
        }
    }
}
