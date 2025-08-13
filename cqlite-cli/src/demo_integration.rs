//! Demo Integration for CQL Query Processor
//!
//! This module provides a demonstration of the enhanced CQL query processing
//! capabilities with sample data and guided examples.

use anyhow::Result;
use colored::Colorize;
use cqlite_core::{Database, Config as CoreConfig};
use std::path::Path;

/// Demo integration showcasing CQL query processor features
pub struct CQLProcessorDemo {
    /// Database instance
    database: Database,
    /// Demo data loaded
    demo_data_loaded: bool,
}

impl CQLProcessorDemo {
    /// Create new demo instance
    pub async fn new(db_path: &Path) -> Result<Self> {
        let config = CoreConfig::default();
        let database = Database::open(db_path, config).await?;
        
        Ok(Self {
            database,
            demo_data_loaded: false,
        })
    }

    /// Run interactive demo
    pub async fn run_demo(&mut self) -> Result<()> {
        self.show_demo_banner();
        
        // Step 1: Load demo data
        self.load_demo_data().await?;
        
        // Step 2: Demonstrate query features
        self.demonstrate_query_features().await?;
        
        // Step 3: Show performance analysis
        self.demonstrate_performance_analysis().await?;
        
        // Step 4: Interactive exploration
        self.interactive_exploration().await?;
        
        Ok(())
    }

    /// Show demo banner
    fn show_demo_banner(&self) {
        println!("{}", "╔═══════════════════════════════════════════════════════╗".cyan());
        println!("{}", "║             CQL Query Processor Demo                 ║".cyan().bold());
        println!("{}", "║          Advanced Cassandra Query Engine             ║".cyan());
        println!("{}", "╚═══════════════════════════════════════════════════════╝".cyan());
        println!();
        println!("{}", "🎯 Demo Features:".green().bold());
        println!("  • Enhanced SELECT query processing");
        println!("  • Performance metrics and optimization");
        println!("  • Interactive schema exploration");
        println!("  • Real-time query analysis");
        println!("  • Advanced result formatting");
        println!();
    }

    /// Load demo data
    async fn load_demo_data(&mut self) -> Result<()> {
        println!("{}", "📂 Loading Demo Data...".yellow().bold());
        
        // Create demo keyspace
        let create_keyspace = "CREATE KEYSPACE demo WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}";
        
        match self.database.execute(create_keyspace).await {
            Ok(_) => println!("✅ Created demo keyspace"),
            Err(_) => println!("ℹ️  Demo keyspace already exists"),
        }

        // Create users table
        let create_users_table = r#"
            CREATE TABLE demo.users (
                id UUID PRIMARY KEY,
                name TEXT,
                email TEXT,
                age INT,
                created_at TIMESTAMP,
                preferences MAP<TEXT, TEXT>,
                tags SET<TEXT>
            )
        "#;

        match self.database.execute(create_users_table).await {
            Ok(_) => println!("✅ Created users table"),
            Err(_) => println!("ℹ️  Users table already exists"),
        }

        // Create events table
        let create_events_table = r#"
            CREATE TABLE demo.events (
                user_id UUID,
                event_time TIMESTAMP,
                event_type TEXT,
                data MAP<TEXT, TEXT>,
                PRIMARY KEY (user_id, event_time)
            ) WITH CLUSTERING ORDER BY (event_time DESC)
        "#;

        match self.database.execute(create_events_table).await {
            Ok(_) => println!("✅ Created events table"),
            Err(_) => println!("ℹ️  Events table already exists"),
        }

        // Insert sample data
        self.insert_sample_data().await?;
        
        self.demo_data_loaded = true;
        println!("🎉 Demo data loaded successfully!");
        println!();
        
        Ok(())
    }

    /// Insert sample data
    async fn insert_sample_data(&self) -> Result<()> {
        let sample_users = vec![
            "INSERT INTO demo.users (id, name, email, age) VALUES (uuid(), 'Alice Johnson', 'alice@example.com', 28)",
            "INSERT INTO demo.users (id, name, email, age) VALUES (uuid(), 'Bob Smith', 'bob@example.com', 34)",
            "INSERT INTO demo.users (id, name, email, age) VALUES (uuid(), 'Carol White', 'carol@example.com', 29)",
            "INSERT INTO demo.users (id, name, email, age) VALUES (uuid(), 'David Brown', 'david@example.com', 31)",
            "INSERT INTO demo.users (id, name, email, age) VALUES (uuid(), 'Eve Davis', 'eve@example.com', 26)",
        ];

        for query in sample_users {
            if let Err(e) = self.database.execute(query).await {
                println!("⚠️  Sample data insert warning: {}", e);
            }
        }

        Ok(())
    }

    /// Demonstrate query features
    async fn demonstrate_query_features(&self) -> Result<()> {
        println!("{}", "🔍 Demonstrating Query Features...".yellow().bold());
        println!();

        // Feature 1: Basic SELECT
        self.demo_feature("Basic SELECT Query", "SELECT * FROM demo.users LIMIT 3").await?;

        // Feature 2: WHERE clause
        self.demo_feature("WHERE Clause", "SELECT name, email FROM demo.users WHERE age > 30").await?;

        // Feature 3: COUNT query
        self.demo_feature("COUNT Query", "SELECT COUNT(*) FROM demo.users").await?;

        // Feature 4: Schema exploration
        self.demo_feature("Schema Exploration", "DESCRIBE TABLE demo.users").await?;

        println!();
        Ok(())
    }

    /// Demonstrate a single query feature
    async fn demo_feature(&self, feature_name: &str, query: &str) -> Result<()> {
        println!("🎯 {}", feature_name.cyan().bold());
        println!("   Query: {}", query.yellow());
        
        let start = std::time::Instant::now();
        match self.database.execute(query).await {
            Ok(result) => {
                let duration = start.elapsed();
                println!("   ✅ Executed in {:.2}ms", duration.as_millis());
                println!("   📊 Returned {} rows", result.rows.len());
                
                // Show first few rows for SELECT queries
                if query.to_uppercase().starts_with("SELECT") && !result.rows.is_empty() {
                    println!("   📄 Sample results:");
                    for (i, row) in result.rows.iter().take(2).enumerate() {
                        println!("      Row {}: {:?}", i + 1, row);
                    }
                    if result.rows.len() > 2 {
                        println!("      ... ({} more rows)", result.rows.len() - 2);
                    }
                }
            }
            Err(e) => {
                println!("   ❌ Error: {}", e);
            }
        }
        println!();
        
        Ok(())
    }

    /// Demonstrate performance analysis
    async fn demonstrate_performance_analysis(&self) -> Result<()> {
        println!("{}", "⚡ Performance Analysis Demo...".yellow().bold());
        println!();

        // Demonstrate timing analysis
        let queries = vec![
            "SELECT COUNT(*) FROM demo.users",
            "SELECT * FROM demo.users",
            "SELECT name FROM demo.users WHERE age > 25",
        ];

        let mut total_time = 0;
        let mut query_times = Vec::new();

        for (i, query) in queries.iter().enumerate() {
            println!("🔍 Query {}: {}", i + 1, query.cyan());
            
            let start = std::time::Instant::now();
            match self.database.execute(query).await {
                Ok(result) => {
                    let duration = start.elapsed();
                    let duration_ms = duration.as_millis();
                    total_time += duration_ms;
                    query_times.push(duration_ms);
                    
                    println!("   ⏱️  Execution time: {}ms", duration_ms);
                    println!("   📊 Rows returned: {}", result.rows.len());
                    
                    // Performance analysis
                    if duration_ms > 100 {
                        println!("   ⚠️  Slow query detected! Consider optimization");
                    } else {
                        println!("   ✅ Good performance");
                    }
                }
                Err(e) => {
                    println!("   ❌ Error: {}", e);
                }
            }
            println!();
        }

        // Summary
        println!("📈 {}", "Performance Summary:".green().bold());
        println!("   Total execution time: {}ms", total_time);
        if !query_times.is_empty() {
            let avg_time = total_time / query_times.len() as u128;
            println!("   Average query time: {}ms", avg_time);
            
            let fastest = query_times.iter().min().unwrap();
            let slowest = query_times.iter().max().unwrap();
            println!("   Fastest query: {}ms", fastest);
            println!("   Slowest query: {}ms", slowest);
        }
        println!();

        Ok(())
    }

    /// Interactive exploration
    async fn interactive_exploration(&self) -> Result<()> {
        println!("{}", "🎮 Interactive Exploration...".yellow().bold());
        println!();
        println!("Try these enhanced REPL commands:");
        println!("  {} - List all tables with metadata", ":tables".green());
        println!("  {} - Show keyspaces", ":keyspaces".green());
        println!("  {} - Describe the users table", ":describe demo.users".green());
        println!("  {} - Show performance metrics", ":timing".green());
        println!("  {} - Clear the screen", ":clear".green());
        println!("  {} - Show command history", ":history".green());
        println!();
        println!("Example queries to try:");
        println!("  {}", "SELECT name, age FROM demo.users ORDER BY age;".yellow());
        println!("  {}", "SELECT * FROM demo.users WHERE age BETWEEN 25 AND 35;".yellow());
        println!("  {}", "SELECT COUNT(*) as user_count FROM demo.users;".yellow());
        println!();
        println!("💡 {}", "Tips:".cyan().bold());
        println!("  • Use :timing to see detailed execution metrics");
        println!("  • LIMIT clauses are automatically added in interactive mode");
        println!("  • Query results are automatically paginated for large sets");
        println!("  • Press Tab for auto-completion (coming soon)");
        println!();

        Ok(())
    }

    /// Check if demo data is loaded
    pub fn is_demo_data_loaded(&self) -> bool {
        self.demo_data_loaded
    }

    /// Get database reference
    pub fn database(&self) -> &Database {
        &self.database
    }
}

/// Quick demo for command-line usage
pub async fn quick_demo(db_path: &Path) -> Result<()> {
    println!("{}", "🚀 CQL Query Processor Quick Demo".green().bold());
    println!();

    let mut demo = CQLProcessorDemo::new(db_path).await?;
    demo.run_demo().await?;

    println!("{}", "✅ Demo completed! Start the REPL to explore further:".green().bold());
    println!("   {}", "cqlite repl".cyan());
    println!();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_demo_creation() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("demo.db");
        
        let demo = CQLProcessorDemo::new(&db_path).await.unwrap();
        assert!(!demo.is_demo_data_loaded());
    }

    #[tokio::test]
    async fn test_demo_data_loading() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("demo.db");
        
        let mut demo = CQLProcessorDemo::new(&db_path).await.unwrap();
        demo.load_demo_data().await.unwrap();
        assert!(demo.is_demo_data_loaded());
    }
}