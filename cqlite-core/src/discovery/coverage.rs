//! Schema coverage calculation and badge generation
//!
//! This module provides functionality for computing schema coverage by comparing
//! discovered SSTables with loaded schemas, and generating coverage badges.

use std::sync::Arc;
use tokio::sync::RwLock;

use crate::error::Result;
use crate::schema::registry::SchemaRegistry;

/// Coverage information for discovered tables
#[derive(Debug, Clone)]
pub struct CoverageInfo {
    /// Total number of tables discovered in data directory
    pub total_tables: usize,
    /// Number of tables with schema definitions loaded
    pub tables_with_schema: usize,
    /// Tables discovered but missing schema definitions
    pub tables_missing_schema: Vec<String>,
    /// Schema definitions loaded but no corresponding data found
    pub schemas_without_data: Vec<String>,
}

impl CoverageInfo {
    /// Calculate coverage percentage
    pub fn coverage_percentage(&self) -> f64 {
        if self.total_tables == 0 {
            return 0.0;
        }
        (self.tables_with_schema as f64 / self.total_tables as f64) * 100.0
    }

    /// Check if there are critical issues
    pub fn has_critical_issues(&self) -> bool {
        // Critical issues: more than 50% of discovered tables missing schema
        if self.total_tables > 0 {
            let missing_pct =
                (self.tables_missing_schema.len() as f64 / self.total_tables as f64) * 100.0;
            missing_pct > 50.0
        } else {
            false
        }
    }
}

/// Coverage badge indicating overall schema coverage status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoverageBadge {
    /// Excellent coverage (≥95%)
    Green,
    /// Acceptable coverage (50-95%)
    Yellow,
    /// Poor coverage (<50% or critical issues)
    Red,
    /// No schema registry provided or no data to analyze
    Unknown,
}

impl CoverageBadge {
    /// Get a human-readable description of the badge
    pub fn description(&self) -> &'static str {
        match self {
            CoverageBadge::Green => "Excellent coverage (≥95%)",
            CoverageBadge::Yellow => "Acceptable coverage (50-95%)",
            CoverageBadge::Red => "Poor coverage (<50% or critical issues)",
            CoverageBadge::Unknown => "Unknown coverage (no schema data)",
        }
    }

    /// Get the badge color name
    pub fn color(&self) -> &'static str {
        match self {
            CoverageBadge::Green => "green",
            CoverageBadge::Yellow => "yellow",
            CoverageBadge::Red => "red",
            CoverageBadge::Unknown => "gray",
        }
    }
}

/// Calculator for schema coverage analysis
pub struct CoverageCalculator {
    schema_registry: Arc<RwLock<SchemaRegistry>>,
}

impl CoverageCalculator {
    /// Create a new coverage calculator with the given schema registry
    pub fn new(schema_registry: Arc<RwLock<SchemaRegistry>>) -> Self {
        Self { schema_registry }
    }

    /// Calculate coverage by comparing discovered tables with loaded schemas
    ///
    /// # Arguments
    ///
    /// * `discovered_tables` - Fully qualified table names discovered in data directory
    ///
    /// # Returns
    ///
    /// Coverage information including tables with/without schema and schemas without data
    pub async fn calculate(&self, discovered_tables: &[String]) -> Result<CoverageInfo> {
        let registry = self.schema_registry.read().await;

        // Get all registered schemas
        let registered_schemas = registry.list_schemas(None).await?;

        // Build a set of registered table names for fast lookup
        let registered_names: Vec<String> = registered_schemas
            .iter()
            .map(|s| format!("{}.{}", s.keyspace, s.table))
            .collect();

        // Compare discovered tables with registered schemas
        let mut tables_with_schema = Vec::new();
        let mut tables_missing_schema = Vec::new();

        for table in discovered_tables {
            // Normalize table name (keyspace.table)
            let normalized = normalize_table_name(table);

            if registered_names.iter().any(|s| {
                let schema_normalized = normalize_table_name(s);
                schema_normalized == normalized
            }) {
                tables_with_schema.push(table.clone());
            } else {
                tables_missing_schema.push(table.clone());
            }
        }

        // Find schemas without corresponding data
        let mut schemas_without_data = Vec::new();
        for schema_name in &registered_names {
            let schema_normalized = normalize_table_name(schema_name);
            if !discovered_tables.iter().any(|t| {
                let table_normalized = normalize_table_name(t);
                table_normalized == schema_normalized
            }) {
                schemas_without_data.push(schema_name.clone());
            }
        }

        Ok(CoverageInfo {
            total_tables: discovered_tables.len(),
            tables_with_schema: tables_with_schema.len(),
            tables_missing_schema,
            schemas_without_data,
        })
    }

    /// Compute coverage badge based on coverage information
    ///
    /// Badge thresholds:
    /// - Green: ≥95% coverage
    /// - Yellow: 50-95% coverage
    /// - Red: <50% coverage or critical issues
    pub fn compute_badge(&self, coverage: &CoverageInfo) -> CoverageBadge {
        if coverage.total_tables == 0 {
            return CoverageBadge::Unknown;
        }

        // Check for critical issues first
        if coverage.has_critical_issues() {
            return CoverageBadge::Red;
        }

        let coverage_pct = coverage.coverage_percentage();

        if coverage_pct >= 95.0 {
            CoverageBadge::Green
        } else if coverage_pct >= 50.0 {
            CoverageBadge::Yellow
        } else {
            CoverageBadge::Red
        }
    }
}

/// Normalize table name to lowercase keyspace.table format
fn normalize_table_name(name: &str) -> String {
    name.to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::registry::{SchemaRegistry, SchemaRegistryConfig};
    use crate::schema::TableSchema;
    use crate::{Config, Platform};
    use std::sync::Arc;
    use tokio::sync::RwLock;

    async fn create_test_registry() -> Arc<RwLock<SchemaRegistry>> {
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());
        let registry_config = SchemaRegistryConfig::default();

        Arc::new(RwLock::new(
            SchemaRegistry::new(registry_config, platform, config)
                .await
                .unwrap(),
        ))
    }

    async fn register_test_schema(
        registry: Arc<RwLock<SchemaRegistry>>,
        keyspace: &str,
        table: &str,
    ) {
        use crate::schema::registry::SchemaSource;

        let schema = TableSchema::new_for_testing(keyspace, table);

        let reg = registry.write().await;
        reg.register_schema(schema, SchemaSource::Manual)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_coverage_info_percentage() {
        let info = CoverageInfo {
            total_tables: 10,
            tables_with_schema: 8,
            tables_missing_schema: vec!["ks.t1".to_string(), "ks.t2".to_string()],
            schemas_without_data: vec![],
        };

        assert_eq!(info.coverage_percentage(), 80.0);
    }

    #[tokio::test]
    async fn test_coverage_info_zero_tables() {
        let info = CoverageInfo {
            total_tables: 0,
            tables_with_schema: 0,
            tables_missing_schema: vec![],
            schemas_without_data: vec![],
        };

        assert_eq!(info.coverage_percentage(), 0.0);
    }

    #[tokio::test]
    async fn test_coverage_info_critical_issues() {
        // Critical: >50% missing schema
        let info = CoverageInfo {
            total_tables: 10,
            tables_with_schema: 4,
            tables_missing_schema: vec![
                "ks.t1".to_string(),
                "ks.t2".to_string(),
                "ks.t3".to_string(),
                "ks.t4".to_string(),
                "ks.t5".to_string(),
                "ks.t6".to_string(),
            ],
            schemas_without_data: vec![],
        };

        assert!(info.has_critical_issues());
    }

    #[tokio::test]
    async fn test_coverage_badge_green() {
        let registry = create_test_registry().await;
        let calculator = CoverageCalculator::new(registry);

        let coverage = CoverageInfo {
            total_tables: 100,
            tables_with_schema: 96,
            tables_missing_schema: vec![],
            schemas_without_data: vec![],
        };

        let badge = calculator.compute_badge(&coverage);
        assert_eq!(badge, CoverageBadge::Green);
        assert_eq!(badge.description(), "Excellent coverage (≥95%)");
        assert_eq!(badge.color(), "green");
    }

    #[tokio::test]
    async fn test_coverage_badge_yellow() {
        let registry = create_test_registry().await;
        let calculator = CoverageCalculator::new(registry);

        let coverage = CoverageInfo {
            total_tables: 100,
            tables_with_schema: 75,
            tables_missing_schema: vec![],
            schemas_without_data: vec![],
        };

        let badge = calculator.compute_badge(&coverage);
        assert_eq!(badge, CoverageBadge::Yellow);
        assert_eq!(badge.description(), "Acceptable coverage (50-95%)");
        assert_eq!(badge.color(), "yellow");
    }

    #[tokio::test]
    async fn test_coverage_badge_red() {
        let registry = create_test_registry().await;
        let calculator = CoverageCalculator::new(registry);

        let coverage = CoverageInfo {
            total_tables: 100,
            tables_with_schema: 30,
            tables_missing_schema: vec![],
            schemas_without_data: vec![],
        };

        let badge = calculator.compute_badge(&coverage);
        assert_eq!(badge, CoverageBadge::Red);
        assert_eq!(
            badge.description(),
            "Poor coverage (<50% or critical issues)"
        );
        assert_eq!(badge.color(), "red");
    }

    #[tokio::test]
    async fn test_coverage_badge_unknown() {
        let registry = create_test_registry().await;
        let calculator = CoverageCalculator::new(registry);

        let coverage = CoverageInfo {
            total_tables: 0,
            tables_with_schema: 0,
            tables_missing_schema: vec![],
            schemas_without_data: vec![],
        };

        let badge = calculator.compute_badge(&coverage);
        assert_eq!(badge, CoverageBadge::Unknown);
        assert_eq!(badge.description(), "Unknown coverage (no schema data)");
        assert_eq!(badge.color(), "gray");
    }

    #[tokio::test]
    async fn test_coverage_calculator() {
        let registry = create_test_registry().await;

        // Register some test schemas
        register_test_schema(registry.clone(), "ks1", "table1").await;
        register_test_schema(registry.clone(), "ks1", "table2").await;
        register_test_schema(registry.clone(), "ks2", "table3").await;

        let calculator = CoverageCalculator::new(registry);

        // Discovered tables (some match schemas, some don't)
        let discovered = vec![
            "ks1.table1".to_string(),
            "ks1.table2".to_string(),
            "ks2.table4".to_string(), // No schema for this
        ];

        let coverage = calculator.calculate(&discovered).await.unwrap();

        assert_eq!(coverage.total_tables, 3);
        assert_eq!(coverage.tables_with_schema, 2);
        assert_eq!(coverage.tables_missing_schema.len(), 1);
        assert!(coverage
            .tables_missing_schema
            .contains(&"ks2.table4".to_string()));
        assert_eq!(coverage.schemas_without_data.len(), 1);
        assert!(coverage
            .schemas_without_data
            .contains(&"ks2.table3".to_string()));
    }

    #[tokio::test]
    async fn test_coverage_calculator_empty_discovered() {
        let registry = create_test_registry().await;
        register_test_schema(registry.clone(), "ks1", "table1").await;

        let calculator = CoverageCalculator::new(registry);
        let discovered: Vec<String> = vec![];

        let coverage = calculator.calculate(&discovered).await.unwrap();

        assert_eq!(coverage.total_tables, 0);
        assert_eq!(coverage.tables_with_schema, 0);
        assert_eq!(coverage.tables_missing_schema.len(), 0);
        assert_eq!(coverage.schemas_without_data.len(), 1);
    }

    #[tokio::test]
    async fn test_coverage_calculator_case_insensitive() {
        let registry = create_test_registry().await;
        register_test_schema(registry.clone(), "MyKS", "MyTable").await;

        let calculator = CoverageCalculator::new(registry);
        let discovered = vec!["myks.mytable".to_string()]; // lowercase

        let coverage = calculator.calculate(&discovered).await.unwrap();

        assert_eq!(coverage.total_tables, 1);
        assert_eq!(coverage.tables_with_schema, 1);
        assert_eq!(coverage.tables_missing_schema.len(), 0);
    }

    #[tokio::test]
    async fn test_normalize_table_name() {
        assert_eq!(normalize_table_name("MyKS.MyTable"), "myks.mytable");
        assert_eq!(normalize_table_name("ks.table"), "ks.table");
        assert_eq!(normalize_table_name("KS.TABLE"), "ks.table");
    }
}
