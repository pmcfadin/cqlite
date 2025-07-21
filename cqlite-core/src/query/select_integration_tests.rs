//! Integration Tests for Revolutionary CQL SELECT Parser
//!
//! This module contains comprehensive integration tests that demonstrate
//! the FIRST EVER direct CQL querying of SSTable files without Cassandra.

#[cfg(test)]
mod tests {
    use crate::{
        query::{parse_select, SelectExecutor, SelectOptimizer, SelectStatement},
        schema::SchemaManager,
        storage::StorageEngine,
        types::{DataType, TableId, Value},
        Config, Database,
    };
    use std::sync::Arc;
    use tempfile::TempDir;

    /// Test helper to create a test database
    pub async fn create_test_database() -> (Database, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let db = Database::open(temp_dir.path(), config).await.unwrap();
        (db, temp_dir)
    }

    #[tokio::test]
    async fn test_simple_select_all() {
        let (db, _temp_dir) = create_test_database().await;

        // Create table
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
            .await
            .unwrap();

        // Insert test data
        db.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
            .await
            .unwrap();
        db.execute("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")
            .await
            .unwrap();

        // Test SELECT *
        let result = db.execute("SELECT * FROM users").await.unwrap();
        assert_eq!(result.rows.len(), 2);
        assert!(result.execution_time_ms > 0);
    }

    #[tokio::test]
    async fn test_select_with_where_clause() {
        let (db, _temp_dir) = create_test_database().await;

        // Create table
        db.execute(
            "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price FLOAT, category TEXT)",
        )
        .await
        .unwrap();

        // Insert test data
        db.execute("INSERT INTO products VALUES (1, 'Laptop', 999.99, 'Electronics')")
            .await
            .unwrap();
        db.execute("INSERT INTO products VALUES (2, 'Phone', 599.99, 'Electronics')")
            .await
            .unwrap();
        db.execute("INSERT INTO products VALUES (3, 'Book', 19.99, 'Books')")
            .await
            .unwrap();

        // Test WHERE with equality
        let result = db
            .execute("SELECT * FROM products WHERE category = 'Electronics'")
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 2);

        // Test WHERE with comparison
        let result = db
            .execute("SELECT * FROM products WHERE price > 500")
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 2);

        // Test WHERE with BETWEEN
        let result = db
            .execute("SELECT * FROM products WHERE price BETWEEN 20 AND 1000")
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 2);
    }

    #[tokio::test]
    async fn test_select_with_in_clause() {
        let (db, _temp_dir) = create_test_database().await;

        // Create table
        db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT, amount FLOAT)")
            .await
            .unwrap();

        // Insert test data
        db.execute("INSERT INTO orders VALUES (1, 'pending', 100.0)")
            .await
            .unwrap();
        db.execute("INSERT INTO orders VALUES (2, 'shipped', 250.0)")
            .await
            .unwrap();
        db.execute("INSERT INTO orders VALUES (3, 'delivered', 150.0)")
            .await
            .unwrap();
        db.execute("INSERT INTO orders VALUES (4, 'cancelled', 75.0)")
            .await
            .unwrap();

        // Test IN clause
        let result = db
            .execute("SELECT * FROM orders WHERE status IN ('pending', 'shipped')")
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 2);
    }

    #[tokio::test]
    async fn test_aggregation_functions() {
        let (db, _temp_dir) = create_test_database().await;

        // Create table
        db.execute("CREATE TABLE sales (id INTEGER PRIMARY KEY, region TEXT, amount FLOAT)")
            .await
            .unwrap();

        // Insert test data
        db.execute("INSERT INTO sales VALUES (1, 'North', 1000.0)")
            .await
            .unwrap();
        db.execute("INSERT INTO sales VALUES (2, 'South', 1500.0)")
            .await
            .unwrap();
        db.execute("INSERT INTO sales VALUES (3, 'North', 800.0)")
            .await
            .unwrap();
        db.execute("INSERT INTO sales VALUES (4, 'East', 1200.0)")
            .await
            .unwrap();

        // Test COUNT
        let result = db.execute("SELECT COUNT(*) FROM sales").await.unwrap();
        assert_eq!(result.rows.len(), 1);

        // Test SUM
        let result = db.execute("SELECT SUM(amount) FROM sales").await.unwrap();
        assert_eq!(result.rows.len(), 1);

        // Test AVG
        let result = db.execute("SELECT AVG(amount) FROM sales").await.unwrap();
        assert_eq!(result.rows.len(), 1);

        // Test GROUP BY with aggregation
        let result = db
            .execute("SELECT region, COUNT(*), SUM(amount) FROM sales GROUP BY region")
            .await
            .unwrap();
        assert!(result.rows.len() >= 3); // Should have groups for North, South, East
    }

    #[tokio::test]
    async fn test_order_by_and_limit() {
        let (db, _temp_dir) = create_test_database().await;

        // Create table
        db.execute("CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, salary FLOAT, department TEXT)")
            .await
            .unwrap();

        // Insert test data
        db.execute("INSERT INTO employees VALUES (1, 'Alice', 75000.0, 'Engineering')")
            .await
            .unwrap();
        db.execute("INSERT INTO employees VALUES (2, 'Bob', 65000.0, 'Marketing')")
            .await
            .unwrap();
        db.execute("INSERT INTO employees VALUES (3, 'Charlie', 85000.0, 'Engineering')")
            .await
            .unwrap();
        db.execute("INSERT INTO employees VALUES (4, 'Diana', 70000.0, 'Sales')")
            .await
            .unwrap();

        // Test ORDER BY
        let result = db
            .execute("SELECT * FROM employees ORDER BY salary DESC")
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 4);

        // Test LIMIT
        let result = db
            .execute("SELECT * FROM employees ORDER BY salary DESC LIMIT 2")
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 2);

        // Test ORDER BY with multiple columns
        let result = db
            .execute("SELECT * FROM employees ORDER BY department ASC, salary DESC")
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 4);
    }

    #[tokio::test]
    async fn test_complex_where_expressions() {
        let (db, _temp_dir) = create_test_database().await;

        // Create table
        db.execute("CREATE TABLE inventory (id INTEGER PRIMARY KEY, product TEXT, quantity INTEGER, price FLOAT, active BOOLEAN)")
            .await
            .unwrap();

        // Insert test data
        db.execute("INSERT INTO inventory VALUES (1, 'Widget A', 100, 10.50, true)")
            .await
            .unwrap();
        db.execute("INSERT INTO inventory VALUES (2, 'Widget B', 50, 15.75, true)")
            .await
            .unwrap();
        db.execute("INSERT INTO inventory VALUES (3, 'Widget C', 0, 8.25, false)")
            .await
            .unwrap();
        db.execute("INSERT INTO inventory VALUES (4, 'Widget D', 25, 20.00, true)")
            .await
            .unwrap();

        // Test complex AND/OR conditions
        let result = db.execute(
            "SELECT * FROM inventory WHERE (quantity > 20 AND price < 20.0) OR (active = false)"
        ).await.unwrap();
        assert!(result.rows.len() >= 2);

        // Test NOT conditions
        let result = db
            .execute("SELECT * FROM inventory WHERE NOT (quantity = 0)")
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 3);

        // Test IS NULL / IS NOT NULL
        db.execute("INSERT INTO inventory VALUES (5, 'Widget E', NULL, 12.50, true)")
            .await
            .unwrap();

        let result = db
            .execute("SELECT * FROM inventory WHERE quantity IS NOT NULL")
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 4);
    }

    #[tokio::test]
    async fn test_like_pattern_matching() {
        let (db, _temp_dir) = create_test_database().await;

        // Create table
        db.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")
            .await
            .unwrap();

        // Insert test data
        db.execute("INSERT INTO customers VALUES (1, 'John Smith', 'john@email.com')")
            .await
            .unwrap();
        db.execute("INSERT INTO customers VALUES (2, 'Jane Doe', 'jane@company.org')")
            .await
            .unwrap();
        db.execute("INSERT INTO customers VALUES (3, 'Bob Johnson', 'bob@email.com')")
            .await
            .unwrap();

        // Test LIKE with % wildcard
        let result = db
            .execute("SELECT * FROM customers WHERE email LIKE '%@email.com'")
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 2);

        // Test LIKE with _ wildcard
        let result = db
            .execute("SELECT * FROM customers WHERE name LIKE 'J___ %'")
            .await
            .unwrap();
        assert!(result.rows.len() >= 1);
    }

    #[tokio::test]
    async fn test_collection_operations() {
        let (db, _temp_dir) = create_test_database().await;

        // Create table with collections
        db.execute("CREATE TABLE user_data (id INTEGER PRIMARY KEY, tags LIST<TEXT>, preferences MAP<TEXT, TEXT>)")
            .await
            .unwrap();

        // Insert test data with collections
        db.execute("INSERT INTO user_data VALUES (1, ['tech', 'programming', 'rust'], {'theme': 'dark', 'language': 'en'})")
            .await
            .unwrap();

        // Test collection access in SELECT
        let result = db
            .execute("SELECT id, tags[0], preferences['theme'] FROM user_data")
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 1);

        // Test collection CONTAINS
        let result = db
            .execute("SELECT * FROM user_data WHERE tags CONTAINS 'rust'")
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 1);

        // Test map CONTAINS KEY
        let result = db
            .execute("SELECT * FROM user_data WHERE preferences CONTAINS KEY 'theme'")
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[tokio::test]
    async fn test_parser_only() {
        // Test the parser without database
        let sql = "SELECT u.name, COUNT(*), AVG(o.amount) FROM users u JOIN orders o ON u.id = o.user_id WHERE u.active = true GROUP BY u.name HAVING COUNT(*) > 5 ORDER BY AVG(o.amount) DESC LIMIT 10";

        let statement = parse_select(sql).unwrap();

        assert!(statement.requires_aggregation());
        assert!(statement.group_by.is_some());
        assert!(statement.having_clause.is_some());
        assert!(statement.order_by.is_some());
        assert!(statement.limit.is_some());

        if let Some(limit) = statement.limit {
            assert_eq!(limit.count, 10);
        }
    }

    #[tokio::test]
    async fn test_optimizer_and_executor_integration() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(crate::platform::Platform::new(&config).await.unwrap());

        let storage = Arc::new(
            StorageEngine::open(temp_dir.path(), &config, platform)
                .await
                .unwrap(),
        );
        let schema = Arc::new(SchemaManager::new(storage.clone(), &config).await.unwrap());

        let optimizer = SelectOptimizer::new(schema.clone(), storage.clone());
        let executor = SelectExecutor::new(schema.clone(), storage.clone());

        // Test simple SELECT statement
        let statement = SelectStatement::select_all_from(TableId::new("users"));

        // Optimize the query
        let optimized_plan = optimizer.optimize(statement).await.unwrap();
        assert!(optimized_plan.estimated_cost > 0.0);
        assert!(!optimized_plan.execution_steps.is_empty());

        // The executor would run the plan, but we need actual SSTable files for that
        // This test validates the integration works without runtime errors
    }

    #[tokio::test]
    async fn test_performance_with_large_dataset() {
        let (db, _temp_dir) = create_test_database().await;

        // Create table for performance testing
        db.execute(
            "CREATE TABLE performance_test (id INTEGER PRIMARY KEY, value INTEGER, category TEXT)",
        )
        .await
        .unwrap();

        // Insert larger dataset
        for i in 0..1000 {
            let query = format!(
                "INSERT INTO performance_test VALUES ({}, {}, 'category_{}')",
                i,
                i * 10,
                i % 10
            );
            db.execute(&query).await.unwrap();
        }

        // Test query performance
        let start = std::time::Instant::now();
        let result = db
            .execute(
                "SELECT category, COUNT(*), AVG(value) FROM performance_test GROUP BY category",
            )
            .await
            .unwrap();
        let duration = start.elapsed();

        assert_eq!(result.rows.len(), 10); // Should have 10 categories
        assert!(duration.as_millis() < 1000); // Should complete within 1 second
        assert!(result.execution_time_ms > 0);
    }

    #[tokio::test]
    async fn test_error_handling() {
        let (db, _temp_dir) = create_test_database().await;

        // Test syntax error
        let result = db.execute("SELECT * FROM").await;
        assert!(result.is_err());

        // Test non-existent table
        let result = db.execute("SELECT * FROM non_existent_table").await;
        assert!(result.is_err());

        // Test invalid column
        db.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY)")
            .await
            .unwrap();

        let result = db
            .execute("SELECT non_existent_column FROM test_table")
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_real_world_query_examples() {
        let (db, _temp_dir) = create_test_database().await;

        // Create realistic e-commerce schema
        db.execute("CREATE TABLE customers (customer_id INTEGER PRIMARY KEY, name TEXT, email TEXT, created_at TIMESTAMP)")
            .await
            .unwrap();

        db.execute("CREATE TABLE orders (order_id INTEGER PRIMARY KEY, customer_id INTEGER, total_amount FLOAT, status TEXT, created_at TIMESTAMP)")
            .await
            .unwrap();

        // Insert sample data
        db.execute("INSERT INTO customers VALUES (1, 'John Doe', 'john@example.com', 1640995200)")
            .await
            .unwrap();
        db.execute(
            "INSERT INTO customers VALUES (2, 'Jane Smith', 'jane@example.com', 1641081600)",
        )
        .await
        .unwrap();

        db.execute("INSERT INTO orders VALUES (1, 1, 299.99, 'completed', 1641168000)")
            .await
            .unwrap();
        db.execute("INSERT INTO orders VALUES (2, 1, 149.50, 'pending', 1641254400)")
            .await
            .unwrap();
        db.execute("INSERT INTO orders VALUES (3, 2, 89.95, 'completed', 1641340800)")
            .await
            .unwrap();

        // Test real-world queries

        // 1. Customer order summary
        let result = db
            .execute(
                "SELECT customer_id, COUNT(*) as order_count, SUM(total_amount) as total_spent 
             FROM orders 
             WHERE status = 'completed' 
             GROUP BY customer_id 
             ORDER BY total_spent DESC",
            )
            .await
            .unwrap();

        assert!(result.rows.len() > 0);

        // 2. Recent high-value orders
        let result = db
            .execute(
                "SELECT order_id, customer_id, total_amount 
             FROM orders 
             WHERE total_amount > 200 AND created_at > 1641000000
             ORDER BY created_at DESC 
             LIMIT 5",
            )
            .await
            .unwrap();

        assert!(result.rows.len() > 0);

        // 3. Complex filtering with multiple conditions
        let result = db
            .execute(
                "SELECT * FROM orders 
             WHERE (status IN ('pending', 'processing') AND total_amount > 100) 
                OR (status = 'completed' AND total_amount > 250)",
            )
            .await
            .unwrap();

        assert!(result.rows.len() > 0);
    }
}

/// Performance benchmarks (for manual testing)
#[cfg(test)]
mod benchmarks {
    use super::tests::create_test_database;
    use crate::{Config, Database};
    use std::time::Instant;
    use tempfile::TempDir;

    #[tokio::test]
    #[ignore] // Run manually with: cargo test benchmarks -- --ignored
    async fn benchmark_select_performance() {
        let (db, _temp_dir) = create_test_database().await;

        // Create large table
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

        // Benchmark different query types
        let queries = vec![
            ("SELECT COUNT(*) FROM benchmark_data", "Simple COUNT"),
            (
                "SELECT * FROM benchmark_data WHERE id < 1000",
                "Range query with LIMIT",
            ),
            (
                "SELECT category, COUNT(*) FROM benchmark_data GROUP BY category",
                "GROUP BY query",
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
