//! M2 SELECT Query Validator
//!
//! This module validates SELECT queries against the M2 supported subset.
//! M2 supports only basic partition-key-based lookups with optional clustering
//! column filters and LIMIT clauses.
//!
//! ## M2 Supported Features
//!
//! - `SELECT` statements with column projections or `*`
//! - `WHERE` clause with partition/primary key equality (`=` operator only)
//! - Optional clustering column equality prefix in `WHERE` clause
//! - Optional `LIMIT` clause
//!
//! ## M2 Unsupported Features
//!
//! - `ORDER BY` clause
//! - `ALLOW FILTERING`
//! - Aggregates: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`
//! - `GROUP BY` clause (SQL-only, not in CQL)
//! - `HAVING` clause (SQL-only, not in CQL)
//! - `JOIN` operations (SQL-only, not in CQL)
//! - Range operators: `>`, `<`, `>=`, `<=`, `!=`, `<>`
//!
//! Note: GROUP BY, HAVING, and JOINs are SQL features that do not exist in CQL.
//! We detect them to provide helpful error messages for users coming from SQL backgrounds.
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use cqlite_core::query::m2_select_validator::M2SelectValidator;
//!
//! let validator = M2SelectValidator;
//! let cql = "SELECT * FROM users WHERE user_id = 123";
//!
//! let result = validator.validate_select(cql)?;
//! assert!(result.has_partition_key_filter);
//! assert!(result.unsupported_features.is_empty());
//! ```

use crate::{Error, Result};

/// M2 SELECT query validator
///
/// This validator performs lightweight string-based detection of unsupported
/// query features for the M2 milestone. It does not perform full CQL parsing.
#[derive(Debug, Clone, Copy)]
pub struct M2SelectValidator;

/// Result of SELECT query validation
#[derive(Debug, Clone, PartialEq)]
pub struct SelectValidationResult {
    /// Whether the query has a partition key filter in WHERE clause
    pub has_partition_key_filter: bool,
    /// Whether the query has clustering column filters in WHERE clause
    pub has_clustering_filters: bool,
    /// Whether the query has a LIMIT clause
    pub has_limit: bool,
    /// List of unsupported features detected in the query
    pub unsupported_features: Vec<UnsupportedFeature>,
}

/// Unsupported query features in M2
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnsupportedFeature {
    /// ORDER BY clause is not supported
    OrderBy,
    /// ALLOW FILTERING is not supported
    AllowFiltering,
    /// Aggregate functions (COUNT, SUM, AVG, MIN, MAX) are not supported
    Aggregates,
    /// GROUP BY clause is not supported
    GroupBy,
    /// HAVING clause is not supported
    Having,
    /// JOIN operations are not supported
    Joins,
    /// Range query operators (>, <, >=, <=, !=, <>) are not supported
    RangeQueries,
}

impl std::fmt::Display for UnsupportedFeature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UnsupportedFeature::OrderBy => write!(f, "ORDER BY"),
            UnsupportedFeature::AllowFiltering => write!(f, "ALLOW FILTERING"),
            UnsupportedFeature::Aggregates => {
                write!(f, "Aggregates (COUNT, SUM, AVG, MIN, MAX)")
            }
            UnsupportedFeature::GroupBy => write!(f, "GROUP BY"),
            UnsupportedFeature::Having => write!(f, "HAVING"),
            UnsupportedFeature::Joins => write!(f, "JOINs"),
            UnsupportedFeature::RangeQueries => {
                write!(f, "Range queries (>, <, >=, <=, !=, <>)")
            }
        }
    }
}

impl M2SelectValidator {
    /// Validate a SELECT query against M2 supported subset
    ///
    /// # Arguments
    ///
    /// * `sql` - The CQL query string to validate
    ///
    /// # Returns
    ///
    /// * `Ok(SelectValidationResult)` if the query is valid or contains detectable unsupported features
    /// * `Err(Error)` if unsupported features are detected
    ///
    /// # Errors
    ///
    /// Returns `Error::UnsupportedQuery` if any unsupported features are detected.
    /// The error message provides helpful guidance on M2 limitations.
    pub fn validate_select(&self, sql: &str) -> Result<SelectValidationResult> {
        let sql_upper = sql.to_uppercase();
        let mut unsupported_features = Vec::new();

        // Detect ORDER BY
        if sql_upper.contains("ORDER BY") {
            unsupported_features.push(UnsupportedFeature::OrderBy);
        }

        // Detect ALLOW FILTERING
        if sql_upper.contains("ALLOW FILTERING") {
            unsupported_features.push(UnsupportedFeature::AllowFiltering);
        }

        // Detect aggregates
        if Self::has_aggregates(&sql_upper) {
            unsupported_features.push(UnsupportedFeature::Aggregates);
        }

        // Detect GROUP BY
        if sql_upper.contains("GROUP BY") {
            unsupported_features.push(UnsupportedFeature::GroupBy);
        }

        // Detect HAVING
        if sql_upper.contains("HAVING") {
            unsupported_features.push(UnsupportedFeature::Having);
        }

        // Detect JOINs
        if Self::has_joins(&sql_upper) {
            unsupported_features.push(UnsupportedFeature::Joins);
        }

        // Detect range operators in WHERE clause
        if Self::has_range_operators(sql) {
            unsupported_features.push(UnsupportedFeature::RangeQueries);
        }

        // If unsupported features found, return error with helpful message
        if !unsupported_features.is_empty() {
            return Err(Self::create_unsupported_error(&unsupported_features));
        }

        // Detect supported features
        let has_partition_key_filter = sql_upper.contains("WHERE");
        let has_clustering_filters = sql_upper.contains("WHERE") && sql_upper.contains("AND");
        let has_limit = sql_upper.contains("LIMIT");

        Ok(SelectValidationResult {
            has_partition_key_filter,
            has_clustering_filters,
            has_limit,
            unsupported_features,
        })
    }

    /// Check if the query contains aggregate functions
    ///
    /// Detects: COUNT, SUM, AVG, MIN, MAX
    fn has_aggregates(sql_upper: &str) -> bool {
        let aggregates = ["COUNT(", "SUM(", "AVG(", "MIN(", "MAX("];
        aggregates.iter().any(|agg| sql_upper.contains(agg))
    }

    /// Check if the query contains JOIN keywords
    ///
    /// Detects: JOIN, INNER JOIN, LEFT JOIN, RIGHT JOIN, FULL JOIN, CROSS JOIN
    fn has_joins(sql_upper: &str) -> bool {
        let joins = [
            " JOIN ",
            " INNER JOIN ",
            " LEFT JOIN ",
            " RIGHT JOIN ",
            " FULL JOIN ",
            " CROSS JOIN ",
        ];
        joins.iter().any(|join| sql_upper.contains(join))
    }

    /// Check if the query contains range operators in WHERE clause
    ///
    /// Detects: >, <, >=, <=, !=, <>
    ///
    /// This is a simplified check that looks for these operators anywhere in
    /// the query after a WHERE clause. It may have false positives (e.g., in
    /// string literals), but this is acceptable for M2's limited scope.
    fn has_range_operators(sql: &str) -> bool {
        // Find WHERE clause position
        let sql_upper = sql.to_uppercase();
        if let Some(where_pos) = sql_upper.find("WHERE") {
            // Check for range operators after WHERE clause
            let after_where = &sql[where_pos..];
            let operators = [">=", "<=", "!=", "<>", ">", "<"];

            // Check each operator, being careful about order (>= before >, etc.)
            operators.iter().any(|op| after_where.contains(op))
        } else {
            false
        }
    }

    /// Create a helpful error message for unsupported features
    fn create_unsupported_error(features: &[UnsupportedFeature]) -> Error {
        let feature_list = features
            .iter()
            .map(|f| f.to_string())
            .collect::<Vec<_>>()
            .join(", ");

        let message = format!(
            "Unsupported query form in M2. Unsupported features: [{}]. \
             M2 supports: SELECT with partition/primary key equality and optional LIMIT. \
             Try narrowing your WHERE clause to use only equality (=) on partition/primary keys.",
            feature_list
        );

        Error::unsupported_query(message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_select_with_partition_key() {
        let validator = M2SelectValidator;
        let sql = "SELECT * FROM users WHERE user_id = 123";

        let result = validator.validate_select(sql).unwrap();

        assert!(result.has_partition_key_filter);
        assert!(!result.has_clustering_filters);
        assert!(!result.has_limit);
        assert!(result.unsupported_features.is_empty());
    }

    #[test]
    fn test_select_with_limit() {
        let validator = M2SelectValidator;
        let sql = "SELECT * FROM users WHERE user_id = 123 LIMIT 10";

        let result = validator.validate_select(sql).unwrap();

        assert!(result.has_partition_key_filter);
        assert!(!result.has_clustering_filters);
        assert!(result.has_limit);
        assert!(result.unsupported_features.is_empty());
    }

    #[test]
    fn test_select_with_clustering_columns() {
        let validator = M2SelectValidator;
        let sql = "SELECT * FROM events WHERE user_id = 123 AND timestamp = '2024-01-01'";

        let result = validator.validate_select(sql).unwrap();

        assert!(result.has_partition_key_filter);
        assert!(result.has_clustering_filters);
        assert!(!result.has_limit);
        assert!(result.unsupported_features.is_empty());
    }

    #[test]
    fn test_select_with_order_by() {
        let validator = M2SelectValidator;
        let sql = "SELECT * FROM users WHERE user_id = 123 ORDER BY name ASC";

        let result = validator.validate_select(sql);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("ORDER BY"));
        assert!(err.to_string().contains("Unsupported query form in M2"));
    }

    #[test]
    fn test_select_with_allow_filtering() {
        let validator = M2SelectValidator;
        let sql = "SELECT * FROM users WHERE email = 'test@example.com' ALLOW FILTERING";

        let result = validator.validate_select(sql);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("ALLOW FILTERING"));
    }

    #[test]
    fn test_select_with_count_aggregate() {
        let validator = M2SelectValidator;
        let sql = "SELECT COUNT(*) FROM users WHERE user_id = 123";

        let result = validator.validate_select(sql);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Aggregates"));
    }

    #[test]
    fn test_select_with_sum_aggregate() {
        let validator = M2SelectValidator;
        let sql = "SELECT SUM(amount) FROM transactions WHERE user_id = 123";

        let result = validator.validate_select(sql);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Aggregates"));
    }

    #[test]
    fn test_select_with_group_by() {
        let validator = M2SelectValidator;
        let sql = "SELECT user_id, COUNT(*) FROM users GROUP BY user_id";

        let result = validator.validate_select(sql);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("GROUP BY"));
    }

    #[test]
    fn test_select_with_having() {
        let validator = M2SelectValidator;
        let sql = "SELECT user_id, COUNT(*) FROM users GROUP BY user_id HAVING COUNT(*) > 5";

        let result = validator.validate_select(sql);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("HAVING"));
    }

    #[test]
    fn test_select_with_join() {
        let validator = M2SelectValidator;
        let sql = "SELECT u.* FROM users u JOIN orders o ON u.user_id = o.user_id";

        let result = validator.validate_select(sql);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("JOIN"));
    }

    #[test]
    fn test_select_with_greater_than() {
        let validator = M2SelectValidator;
        let sql = "SELECT * FROM users WHERE age > 18";

        let result = validator.validate_select(sql);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Range queries"));
    }

    #[test]
    fn test_select_with_less_than_or_equal() {
        let validator = M2SelectValidator;
        let sql = "SELECT * FROM users WHERE age <= 65";

        let result = validator.validate_select(sql);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Range queries"));
    }

    #[test]
    fn test_select_with_not_equal() {
        let validator = M2SelectValidator;
        let sql = "SELECT * FROM users WHERE status != 'deleted'";

        let result = validator.validate_select(sql);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Range queries"));
    }

    #[test]
    fn test_select_with_not_equal_alternative() {
        let validator = M2SelectValidator;
        let sql = "SELECT * FROM users WHERE status <> 'deleted'";

        let result = validator.validate_select(sql);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Range queries"));
    }

    #[test]
    fn test_select_with_multiple_unsupported_features() {
        let validator = M2SelectValidator;
        let sql =
            "SELECT COUNT(*) FROM users WHERE age > 18 GROUP BY country ORDER BY COUNT(*) DESC";

        let result = validator.validate_select(sql);

        assert!(result.is_err());
        let err = result.unwrap_err();
        let err_msg = err.to_string();

        // Should mention multiple features
        assert!(err_msg.contains("ORDER BY"));
        assert!(err_msg.contains("Aggregates"));
        assert!(err_msg.contains("GROUP BY"));
        assert!(err_msg.contains("Range queries"));
    }

    #[test]
    fn test_case_insensitive_detection() {
        let validator = M2SelectValidator;

        // Test lowercase
        let sql_lower = "select * from users where user_id = 123 order by name";
        let result = validator.validate_select(sql_lower);
        assert!(result.is_err());

        // Test mixed case
        let sql_mixed = "SeLeCt * FrOm users WhErE user_id = 123 OrDeR bY name";
        let result = validator.validate_select(sql_mixed);
        assert!(result.is_err());
    }

    #[test]
    fn test_unsupported_feature_display() {
        assert_eq!(UnsupportedFeature::OrderBy.to_string(), "ORDER BY");
        assert_eq!(
            UnsupportedFeature::AllowFiltering.to_string(),
            "ALLOW FILTERING"
        );
        assert_eq!(
            UnsupportedFeature::Aggregates.to_string(),
            "Aggregates (COUNT, SUM, AVG, MIN, MAX)"
        );
        assert_eq!(UnsupportedFeature::GroupBy.to_string(), "GROUP BY");
        assert_eq!(UnsupportedFeature::Having.to_string(), "HAVING");
        assert_eq!(UnsupportedFeature::Joins.to_string(), "JOINs");
        assert_eq!(
            UnsupportedFeature::RangeQueries.to_string(),
            "Range queries (>, <, >=, <=, !=, <>)"
        );
    }

    #[test]
    fn test_validation_result_equality() {
        let result1 = SelectValidationResult {
            has_partition_key_filter: true,
            has_clustering_filters: false,
            has_limit: true,
            unsupported_features: vec![],
        };

        let result2 = SelectValidationResult {
            has_partition_key_filter: true,
            has_clustering_filters: false,
            has_limit: true,
            unsupported_features: vec![],
        };

        assert_eq!(result1, result2);
    }

    #[test]
    fn test_all_aggregate_functions() {
        let validator = M2SelectValidator;

        for aggregate in &["COUNT", "SUM", "AVG", "MIN", "MAX"] {
            let sql = format!("SELECT {}(*) FROM users WHERE user_id = 123", aggregate);
            let result = validator.validate_select(&sql);
            assert!(result.is_err(), "Should detect {} aggregate", aggregate);
        }
    }

    #[test]
    fn test_all_join_types() {
        let validator = M2SelectValidator;

        for join_type in &[
            "JOIN",
            "INNER JOIN",
            "LEFT JOIN",
            "RIGHT JOIN",
            "FULL JOIN",
            "CROSS JOIN",
        ] {
            let sql = format!(
                "SELECT * FROM users {} orders ON users.id = orders.user_id",
                join_type
            );
            let result = validator.validate_select(&sql);
            assert!(result.is_err(), "Should detect {} join", join_type);
        }
    }

    #[test]
    fn test_all_range_operators() {
        let validator = M2SelectValidator;

        for operator in &[">", "<", ">=", "<=", "!=", "<>"] {
            let sql = format!("SELECT * FROM users WHERE age {} 18", operator);
            let result = validator.validate_select(&sql);
            assert!(
                result.is_err(),
                "Should detect range operator: {}",
                operator
            );
        }
    }

    #[test]
    fn test_select_without_where() {
        let validator = M2SelectValidator;
        let sql = "SELECT * FROM users";

        let result = validator.validate_select(sql).unwrap();

        assert!(!result.has_partition_key_filter);
        assert!(!result.has_clustering_filters);
        assert!(!result.has_limit);
        assert!(result.unsupported_features.is_empty());
    }

    #[test]
    fn test_complex_valid_query() {
        let validator = M2SelectValidator;
        let sql = "SELECT user_id, name, email FROM users \
                   WHERE user_id = 123 AND status = 'active' LIMIT 100";

        let result = validator.validate_select(sql).unwrap();

        assert!(result.has_partition_key_filter);
        assert!(result.has_clustering_filters);
        assert!(result.has_limit);
        assert!(result.unsupported_features.is_empty());
    }
}
