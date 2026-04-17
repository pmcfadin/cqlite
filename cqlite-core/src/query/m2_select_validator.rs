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

/// Aggregate function call prefixes detected as unsupported.
const AGGREGATE_PREFIXES: &[&str] = &["COUNT(", "SUM(", "AVG(", "MIN(", "MAX("];

/// JOIN keyword variants detected as unsupported. Each is padded with spaces
/// so we don't flag identifiers that merely contain "JOIN".
const JOIN_KEYWORDS: &[&str] = &[
    " JOIN ",
    " INNER JOIN ",
    " LEFT JOIN ",
    " RIGHT JOIN ",
    " FULL JOIN ",
    " CROSS JOIN ",
];

/// Range operators detected in (or after) the WHERE clause.
const RANGE_OPERATORS: &[&str] = &[">=", "<=", "!=", "<>", ">", "<"];

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

impl UnsupportedFeature {
    fn label(self) -> &'static str {
        match self {
            UnsupportedFeature::OrderBy => "ORDER BY",
            UnsupportedFeature::AllowFiltering => "ALLOW FILTERING",
            UnsupportedFeature::Aggregates => "Aggregates (COUNT, SUM, AVG, MIN, MAX)",
            UnsupportedFeature::GroupBy => "GROUP BY",
            UnsupportedFeature::Having => "HAVING",
            UnsupportedFeature::Joins => "JOINs",
            UnsupportedFeature::RangeQueries => "Range queries (>, <, >=, <=, !=, <>)",
        }
    }
}

impl std::fmt::Display for UnsupportedFeature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.label())
    }
}

impl M2SelectValidator {
    /// Validate a SELECT query against M2 supported subset
    ///
    /// # Arguments
    ///
    /// * `cql` - The CQL query string to validate
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
    pub fn validate_select(&self, cql: &str) -> Result<SelectValidationResult> {
        let cql_upper = cql.to_uppercase();
        let where_pos = cql_upper.find("WHERE");

        // Substring-based unsupported-feature checks. Each rule is a
        // (predicate, feature) pair so the dispatch is data-driven.
        let rules: &[(bool, UnsupportedFeature)] = &[
            (cql_upper.contains("ORDER BY"), UnsupportedFeature::OrderBy),
            (
                cql_upper.contains("ALLOW FILTERING"),
                UnsupportedFeature::AllowFiltering,
            ),
            (
                AGGREGATE_PREFIXES.iter().any(|p| cql_upper.contains(p)),
                UnsupportedFeature::Aggregates,
            ),
            (cql_upper.contains("GROUP BY"), UnsupportedFeature::GroupBy),
            (cql_upper.contains("HAVING"), UnsupportedFeature::Having),
            (
                JOIN_KEYWORDS.iter().any(|j| cql_upper.contains(j)),
                UnsupportedFeature::Joins,
            ),
            (
                has_range_operator_after(&cql_upper, where_pos),
                UnsupportedFeature::RangeQueries,
            ),
        ];

        let unsupported_features: Vec<UnsupportedFeature> = rules
            .iter()
            .filter_map(|(hit, feat)| hit.then_some(*feat))
            .collect();

        if !unsupported_features.is_empty() {
            return Err(unsupported_query_error(&unsupported_features));
        }

        let has_where = where_pos.is_some();
        Ok(SelectValidationResult {
            has_partition_key_filter: has_where,
            has_clustering_filters: has_where && cql_upper.contains("AND"),
            has_limit: cql_upper.contains("LIMIT"),
            unsupported_features,
        })
    }
}

/// Detect range operators (`>`, `<`, `>=`, `<=`, `!=`, `<>`) at or after the
/// WHERE clause. Returns false when there is no WHERE clause.
///
/// This is a substring check; it may flag operators inside string literals.
/// Acceptable for M2's limited scope.
fn has_range_operator_after(cql_upper: &str, where_pos: Option<usize>) -> bool {
    match where_pos {
        Some(pos) => {
            let after_where = &cql_upper[pos..];
            RANGE_OPERATORS.iter().any(|op| after_where.contains(op))
        }
        None => false,
    }
}

fn unsupported_query_error(features: &[UnsupportedFeature]) -> Error {
    let feature_list = features
        .iter()
        .map(|f| f.label())
        .collect::<Vec<_>>()
        .join(", ");

    Error::unsupported_query(format!(
        "Unsupported query form in M2. Unsupported features: [{}]. \
         M2 supports: SELECT with partition/primary key equality and optional LIMIT. \
         Try narrowing your WHERE clause to use only equality (=) on partition/primary keys.",
        feature_list
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_select_with_partition_key() {
        let validator = M2SelectValidator;
        let cql = "SELECT * FROM users WHERE user_id = 123";

        let result = validator.validate_select(cql).unwrap();

        assert!(result.has_partition_key_filter);
        assert!(!result.has_clustering_filters);
        assert!(!result.has_limit);
        assert!(result.unsupported_features.is_empty());
    }

    #[test]
    fn test_select_with_limit() {
        let validator = M2SelectValidator;
        let cql = "SELECT * FROM users WHERE user_id = 123 LIMIT 10";

        let result = validator.validate_select(cql).unwrap();

        assert!(result.has_partition_key_filter);
        assert!(!result.has_clustering_filters);
        assert!(result.has_limit);
        assert!(result.unsupported_features.is_empty());
    }

    #[test]
    fn test_select_with_clustering_columns() {
        let validator = M2SelectValidator;
        let cql = "SELECT * FROM events WHERE user_id = 123 AND timestamp = '2024-01-01'";

        let result = validator.validate_select(cql).unwrap();

        assert!(result.has_partition_key_filter);
        assert!(result.has_clustering_filters);
        assert!(!result.has_limit);
        assert!(result.unsupported_features.is_empty());
    }

    #[test]
    fn test_select_with_order_by() {
        let validator = M2SelectValidator;
        let cql = "SELECT * FROM users WHERE user_id = 123 ORDER BY name ASC";

        let result = validator.validate_select(cql);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("ORDER BY"));
        assert!(err.to_string().contains("Unsupported query form in M2"));
    }

    #[test]
    fn test_select_with_allow_filtering() {
        let validator = M2SelectValidator;
        let cql = "SELECT * FROM users WHERE email = 'test@example.com' ALLOW FILTERING";

        let result = validator.validate_select(cql);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("ALLOW FILTERING"));
    }

    #[test]
    fn test_select_with_count_aggregate() {
        let validator = M2SelectValidator;
        let cql = "SELECT COUNT(*) FROM users WHERE user_id = 123";

        let result = validator.validate_select(cql);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Aggregates"));
    }

    #[test]
    fn test_select_with_sum_aggregate() {
        let validator = M2SelectValidator;
        let cql = "SELECT SUM(amount) FROM transactions WHERE user_id = 123";

        let result = validator.validate_select(cql);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Aggregates"));
    }

    #[test]
    fn test_select_with_group_by() {
        let validator = M2SelectValidator;
        let cql = "SELECT user_id, COUNT(*) FROM users GROUP BY user_id";

        let result = validator.validate_select(cql);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("GROUP BY"));
    }

    #[test]
    fn test_select_with_having() {
        let validator = M2SelectValidator;
        let cql = "SELECT user_id, COUNT(*) FROM users GROUP BY user_id HAVING COUNT(*) > 5";

        let result = validator.validate_select(cql);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("HAVING"));
    }

    #[test]
    fn test_select_with_join() {
        let validator = M2SelectValidator;
        let cql = "SELECT u.* FROM users u JOIN orders o ON u.user_id = o.user_id";

        let result = validator.validate_select(cql);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("JOIN"));
    }

    #[test]
    fn test_select_with_greater_than() {
        let validator = M2SelectValidator;
        let cql = "SELECT * FROM users WHERE age > 18";

        let result = validator.validate_select(cql);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Range queries"));
    }

    #[test]
    fn test_select_with_less_than_or_equal() {
        let validator = M2SelectValidator;
        let cql = "SELECT * FROM users WHERE age <= 65";

        let result = validator.validate_select(cql);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Range queries"));
    }

    #[test]
    fn test_select_with_not_equal() {
        let validator = M2SelectValidator;
        let cql = "SELECT * FROM users WHERE status != 'deleted'";

        let result = validator.validate_select(cql);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Range queries"));
    }

    #[test]
    fn test_select_with_not_equal_alternative() {
        let validator = M2SelectValidator;
        let cql = "SELECT * FROM users WHERE status <> 'deleted'";

        let result = validator.validate_select(cql);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Range queries"));
    }

    #[test]
    fn test_select_with_multiple_unsupported_features() {
        let validator = M2SelectValidator;
        let cql =
            "SELECT COUNT(*) FROM users WHERE age > 18 GROUP BY country ORDER BY COUNT(*) DESC";

        let result = validator.validate_select(cql);

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
        let cql_lower = "select * from users where user_id = 123 order by name";
        let result = validator.validate_select(cql_lower);
        assert!(result.is_err());

        // Test mixed case
        let cql_mixed = "SeLeCt * FrOm users WhErE user_id = 123 OrDeR bY name";
        let result = validator.validate_select(cql_mixed);
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
            let cql = format!("SELECT {}(*) FROM users WHERE user_id = 123", aggregate);
            let result = validator.validate_select(&cql);
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
            let cql = format!(
                "SELECT * FROM users {} orders ON users.id = orders.user_id",
                join_type
            );
            let result = validator.validate_select(&cql);
            assert!(result.is_err(), "Should detect {} join", join_type);
        }
    }

    #[test]
    fn test_all_range_operators() {
        let validator = M2SelectValidator;

        for operator in &[">", "<", ">=", "<=", "!=", "<>"] {
            let cql = format!("SELECT * FROM users WHERE age {} 18", operator);
            let result = validator.validate_select(&cql);
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
        let cql = "SELECT * FROM users";

        let result = validator.validate_select(cql).unwrap();

        assert!(!result.has_partition_key_filter);
        assert!(!result.has_clustering_filters);
        assert!(!result.has_limit);
        assert!(result.unsupported_features.is_empty());
    }

    #[test]
    fn test_complex_valid_query() {
        let validator = M2SelectValidator;
        let cql = "SELECT user_id, name, email FROM users \
                   WHERE user_id = 123 AND status = 'active' LIMIT 100";

        let result = validator.validate_select(cql).unwrap();

        assert!(result.has_partition_key_filter);
        assert!(result.has_clustering_filters);
        assert!(result.has_limit);
        assert!(result.unsupported_features.is_empty());
    }
}
