//! WHERE-clause binding, predicate extraction, and tombstone helpers.
//!
//! These helpers support the mutation builders in `builders.rs`: extracting
//! equality/range predicates from CQL WHERE clauses, resolving them into
//! partition/clustering key values, building range tombstones, validating the
//! statement table, and reading `USING TIMESTAMP`/`TTL` clauses.

#[cfg(feature = "write-support")]
use super::codec::expression_to_value;
#[cfg(feature = "write-support")]
use crate::cql::ast::{CqlBinaryOperator, CqlExpression, CqlLiteral, CqlTable, CqlUsing};
#[cfg(feature = "write-support")]
use crate::schema::{CqlType, TableSchema};
#[cfg(feature = "write-support")]
use crate::storage::write_engine::mutation::{ClusteringBound, ClusteringKey, RangeTombstone};
#[cfg(feature = "write-support")]
use crate::types::Value;
#[cfg(feature = "write-support")]
use crate::Error;

/// Return the current wall-clock time as seconds since Unix epoch, cast to i32.
///
/// This is the correct value for `local_deletion_time` in tombstones.  It must
/// reflect real calendar time so that Cassandra's GC-grace expiry logic works
/// correctly; using a logical CQL timestamp instead would break that invariant.
///
/// Returns 0 on the extremely unlikely event that the system clock is before
/// the Unix epoch (e.g. test environments with a mocked clock).
#[cfg(feature = "write-support")]
pub(super) fn wall_clock_local_deletion_time() -> i32 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i32)
        .unwrap_or(0)
}

/// Extract `(column_name, value_expression)` pairs from a WHERE clause expression.
///
/// Supports AND-chained equality predicates: `col1 = val1 AND col2 = val2 ...`.
///
/// # Errors
///
/// Returns `Error::InvalidInput` for non-equality predicates or unexpected expression forms.
#[cfg(feature = "write-support")]
pub(super) fn extract_where_bindings(
    expr: &CqlExpression,
) -> Result<Vec<(String, CqlExpression)>, Error> {
    let mut bindings = Vec::new();
    collect_equality_bindings(expr, &mut bindings)?;
    Ok(bindings)
}

/// Recursively collect `column = value` bindings from an AND-chained expression tree.
#[cfg(feature = "write-support")]
fn collect_equality_bindings(
    expr: &CqlExpression,
    bindings: &mut Vec<(String, CqlExpression)>,
) -> Result<(), Error> {
    match expr {
        CqlExpression::Binary {
            left,
            operator: CqlBinaryOperator::And,
            right,
        } => {
            collect_equality_bindings(left, bindings)?;
            collect_equality_bindings(right, bindings)?;
        }
        CqlExpression::Binary {
            left,
            operator: CqlBinaryOperator::Eq,
            right,
        } => match left.as_ref() {
            CqlExpression::Column(col_id) => {
                bindings.push((col_id.name.to_lowercase(), (**right).clone()));
            }
            _ => {
                return Err(Error::InvalidInput(
                        "WHERE clause equality predicate must have a column reference on the left-hand side".to_string(),
                    ));
            }
        },
        _ => {
            return Err(Error::InvalidInput(
                "WHERE clause must consist of equality predicates joined with AND".to_string(),
            ));
        }
    }
    Ok(())
}

/// A single range predicate extracted from a DELETE WHERE clause (e.g. `ck > 'a'`).
#[cfg(feature = "write-support")]
pub(super) struct RangePredicate {
    column: String,
    operator: CqlBinaryOperator,
    value: CqlExpression,
}

/// Equality and range predicates extracted from a DELETE WHERE clause.
#[cfg(feature = "write-support")]
pub(super) struct DeletePredicates {
    pub(super) equality_bindings: Vec<(String, CqlExpression)>,
    pub(super) range_predicates: Vec<RangePredicate>,
}

/// Extract equality and range predicates from a DELETE WHERE clause expression.
///
/// Supports AND-chained equality predicates (`col = val`) and range predicates
/// (`col > val`, `col >= val`, `col < val`, `col <= val`).
#[cfg(feature = "write-support")]
pub(super) fn extract_delete_predicates(expr: &CqlExpression) -> Result<DeletePredicates, Error> {
    let mut result = DeletePredicates {
        equality_bindings: Vec::new(),
        range_predicates: Vec::new(),
    };
    collect_delete_predicates(expr, &mut result)?;
    Ok(result)
}

/// Recursively collect equality and range predicates from an AND-chained expression tree.
#[cfg(feature = "write-support")]
fn collect_delete_predicates(
    expr: &CqlExpression,
    result: &mut DeletePredicates,
) -> Result<(), Error> {
    match expr {
        CqlExpression::Binary {
            left,
            operator: CqlBinaryOperator::And,
            right,
        } => {
            collect_delete_predicates(left, result)?;
            collect_delete_predicates(right, result)?;
        }
        CqlExpression::Binary {
            left,
            operator: CqlBinaryOperator::Eq,
            right,
        } => match left.as_ref() {
            CqlExpression::Column(col_id) => {
                result
                    .equality_bindings
                    .push((col_id.name.to_lowercase(), (**right).clone()));
            }
            _ => {
                return Err(Error::InvalidInput(
                    "WHERE clause predicate must have a column reference on the left-hand side"
                        .to_string(),
                ));
            }
        },
        CqlExpression::Binary {
            left,
            operator,
            right,
        } if matches!(
            operator,
            CqlBinaryOperator::Lt
                | CqlBinaryOperator::Le
                | CqlBinaryOperator::Gt
                | CqlBinaryOperator::Ge
        ) =>
        {
            match left.as_ref() {
                CqlExpression::Column(col_id) => {
                    result.range_predicates.push(RangePredicate {
                        column: col_id.name.to_lowercase(),
                        operator: operator.clone(),
                        value: (**right).clone(),
                    });
                }
                _ => {
                    return Err(Error::InvalidInput(
                        "WHERE clause predicate must have a column reference on the left-hand side"
                            .to_string(),
                    ));
                }
            }
        }
        _ => {
            return Err(Error::InvalidInput(
                "DELETE WHERE clause must consist of equality or range predicates joined with AND"
                    .to_string(),
            ));
        }
    }
    Ok(())
}

/// Build `RangeTombstone` values from a set of range predicates.
///
/// **Limitation**: This currently only produces correct results for tables with
/// a single clustering key column. For multi-column clustering keys, each bound
/// is constructed from a single column's value, which may not produce the
/// intended composite range.
///
/// All range predicates must reference clustering key columns. The function
/// produces a single `RangeTombstone` covering the intersection of all bounds.
#[cfg(feature = "write-support")]
pub(super) fn build_range_tombstones(
    range_predicates: &[RangePredicate],
    schema: &TableSchema,
    timestamp_micros: i64,
) -> Result<Vec<RangeTombstone>, Error> {
    let ordered_ck = schema.ordered_clustering_keys();
    let ck_names: Vec<String> = ordered_ck.iter().map(|c| c.name.to_lowercase()).collect();

    // Validate all range predicates reference clustering columns
    for pred in range_predicates {
        if !ck_names.contains(&pred.column) {
            return Err(Error::InvalidInput(format!(
                "Range predicate on non-clustering column '{}'; only clustering key columns support range deletions",
                pred.column
            )));
        }
    }

    // Build bounds: find lower and upper for each clustering column
    let mut lower_bound: Option<ClusteringBound> = None;
    let mut upper_bound: Option<ClusteringBound> = None;

    for pred in range_predicates {
        let ck_col = ordered_ck
            .iter()
            .find(|c| c.name.to_lowercase() == pred.column)
            .ok_or_else(|| {
                Error::InvalidInput(format!(
                    "Internal: clustering column '{}' missing after validation",
                    pred.column
                ))
            })?;
        let cql_type = CqlType::parse(&ck_col.data_type)?;
        let value = expression_to_value(&pred.value, &cql_type)?;
        let ck = ClusteringKey::new(vec![(ck_col.name.clone(), value)]);

        match pred.operator {
            CqlBinaryOperator::Gt => {
                lower_bound = Some(ClusteringBound::Exclusive(ck));
            }
            CqlBinaryOperator::Ge => {
                lower_bound = Some(ClusteringBound::Inclusive(ck));
            }
            CqlBinaryOperator::Lt => {
                upper_bound = Some(ClusteringBound::Exclusive(ck));
            }
            CqlBinaryOperator::Le => {
                upper_bound = Some(ClusteringBound::Inclusive(ck));
            }
            _ => unreachable!("only Lt/Le/Gt/Ge reach build_range_tombstones"),
        }
    }

    Ok(vec![RangeTombstone {
        start: lower_bound.unwrap_or(ClusteringBound::Bottom),
        end: upper_bound.unwrap_or(ClusteringBound::Top),
        deletion_time: timestamp_micros,
        local_deletion_time: wall_clock_local_deletion_time(),
    }])
}

/// Resolved partition key and clustering key columns from a WHERE clause.
#[cfg(feature = "write-support")]
pub(super) struct ResolvedKeys {
    pub(super) partition: Vec<(String, Value)>,
    pub(super) clustering: Vec<(String, Value)>,
}

/// Separate WHERE clause bindings into partition key values and clustering key values.
///
/// Partition key columns are required; an error is returned if any are missing.
/// Clustering key columns are optional (partial WHERE clauses are valid for DELETE).
///
/// # Errors
///
/// Returns `Error::InvalidInput` when a partition key column is missing from the bindings.
#[cfg(feature = "write-support")]
pub(super) fn resolve_key_bindings(
    bindings: &[(String, CqlExpression)],
    schema: &TableSchema,
) -> Result<ResolvedKeys, Error> {
    let ordered_pk = schema.ordered_partition_keys();
    let ordered_ck = schema.ordered_clustering_keys();

    // Resolve partition key values (required)
    let mut pk_columns: Vec<(String, Value)> = Vec::with_capacity(ordered_pk.len());
    for pk_col in &ordered_pk {
        let col_name_lc = pk_col.name.to_lowercase();
        let (_, expr) = bindings
            .iter()
            .find(|(name, _)| *name == col_name_lc)
            .ok_or_else(|| {
                Error::InvalidInput(format!(
                    "Partition key column '{}' is missing from WHERE clause",
                    pk_col.name
                ))
            })?;
        let cql_type = CqlType::parse(&pk_col.data_type)?;
        let value = expression_to_value(expr, &cql_type)?;
        pk_columns.push((pk_col.name.clone(), value));
    }

    // Resolve clustering key values (optional)
    let mut ck_columns: Vec<(String, Value)> = Vec::with_capacity(ordered_ck.len());
    for ck_col in &ordered_ck {
        let col_name_lc = ck_col.name.to_lowercase();
        if let Some((_, expr)) = bindings.iter().find(|(name, _)| *name == col_name_lc) {
            let cql_type = CqlType::parse(&ck_col.data_type)?;
            let value = expression_to_value(expr, &cql_type)?;
            ck_columns.push((ck_col.name.clone(), value));
        }
    }

    Ok(ResolvedKeys {
        partition: pk_columns,
        clustering: ck_columns,
    })
}

/// Validate that the statement's table reference matches the provided schema.
///
/// # Errors
///
/// Returns `Error::InvalidInput` if the table name or keyspace does not match.
#[cfg(feature = "write-support")]
pub(super) fn validate_table(table: &CqlTable, schema: &TableSchema) -> Result<(), Error> {
    if let Some(ks) = &table.keyspace {
        if !ks.name.eq_ignore_ascii_case(&schema.keyspace) {
            return Err(Error::InvalidInput(format!(
                "Statement targets keyspace '{}' but schema is for '{}'",
                ks.name, schema.keyspace
            )));
        }
    }
    if !table.name.name.eq_ignore_ascii_case(&schema.table) {
        return Err(Error::InvalidInput(format!(
            "Statement targets table '{}' but schema is for '{}'",
            table.name.name, schema.table
        )));
    }
    Ok(())
}

/// Extract the timestamp from a USING clause.
///
/// If no USING TIMESTAMP is present, returns the current time in microseconds
/// since the Unix epoch.
///
/// # Errors
///
/// Returns `Error::InvalidInput` if the timestamp expression is not an integer literal.
#[cfg(feature = "write-support")]
pub(super) fn extract_timestamp(using: &Option<CqlUsing>) -> Result<i64, Error> {
    if let Some(u) = using {
        if let Some(ts_expr) = &u.timestamp {
            match ts_expr {
                CqlExpression::Literal(CqlLiteral::Integer(ts)) => return Ok(*ts),
                _ => {
                    return Err(Error::InvalidInput(
                        "USING TIMESTAMP requires an integer literal".to_string(),
                    ))
                }
            }
        }
    }
    // Default: current time in microseconds
    let micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| Error::InvalidInput(format!("System clock error: {}", e)))?
        .as_micros();
    // Cast to i64; saturate if value exceeds i64::MAX (will not happen before year 292k)
    Ok(micros.min(i64::MAX as u128) as i64)
}

/// Extract the TTL from a USING clause.
///
/// Returns `None` if no USING TTL is present.
///
/// # Errors
///
/// Returns `Error::InvalidInput` if the TTL expression is not an integer literal or
/// the value overflows `u32`.
#[cfg(feature = "write-support")]
pub(super) fn extract_ttl(using: &Option<CqlUsing>) -> Result<Option<u32>, Error> {
    if let Some(u) = using {
        if let Some(ttl_expr) = &u.ttl {
            match ttl_expr {
                CqlExpression::Literal(CqlLiteral::Integer(ttl)) => {
                    let v = u32::try_from(*ttl).map_err(|_| {
                        Error::InvalidInput(format!("TTL value {} is out of range for u32", ttl))
                    })?;
                    return Ok(Some(v));
                }
                _ => {
                    return Err(Error::InvalidInput(
                        "USING TTL requires an integer literal".to_string(),
                    ))
                }
            }
        }
    }
    Ok(None)
}

#[cfg(all(test, feature = "write-support"))]
mod tests {
    use super::*;
    use crate::cql::ast::{CqlBinaryOperator, CqlExpression, CqlIdentifier, CqlLiteral};

    // ── shared WHERE-clause fixture (used across binding tests) ────────────────

    fn make_where_pk_and_ck() -> CqlExpression {
        // WHERE id = <uuid> AND ts = <timestamp>
        CqlExpression::Binary {
            left: Box::new(CqlExpression::Binary {
                left: Box::new(CqlExpression::Column(CqlIdentifier {
                    name: "id".into(),
                    quoted: false,
                })),
                operator: CqlBinaryOperator::Eq,
                right: Box::new(CqlExpression::Literal(CqlLiteral::Uuid(
                    "550e8400-e29b-41d4-a716-446655440000".into(),
                ))),
            }),
            operator: CqlBinaryOperator::And,
            right: Box::new(CqlExpression::Binary {
                left: Box::new(CqlExpression::Column(CqlIdentifier {
                    name: "ts".into(),
                    quoted: false,
                })),
                operator: CqlBinaryOperator::Eq,
                right: Box::new(CqlExpression::Literal(CqlLiteral::Integer(
                    1_704_067_200_000,
                ))),
            }),
        }
    }

    #[test]
    fn test_where_bindings_single_eq() {
        let expr = CqlExpression::Binary {
            left: Box::new(CqlExpression::Column(CqlIdentifier::new("id"))),
            operator: CqlBinaryOperator::Eq,
            right: Box::new(CqlExpression::Literal(CqlLiteral::Integer(42))),
        };
        let bindings = extract_where_bindings(&expr).unwrap();
        assert_eq!(bindings.len(), 1);
        assert_eq!(bindings[0].0, "id");
    }

    #[test]
    fn test_where_bindings_and_chain() {
        let bindings = extract_where_bindings(&make_where_pk_and_ck()).unwrap();
        assert_eq!(bindings.len(), 2);
        let names: Vec<_> = bindings.iter().map(|(n, _)| n.as_str()).collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"ts"));
    }

    #[test]
    fn test_where_bindings_non_eq_rejected() {
        let expr = CqlExpression::Binary {
            left: Box::new(CqlExpression::Column(CqlIdentifier::new("age"))),
            operator: CqlBinaryOperator::Gt,
            right: Box::new(CqlExpression::Literal(CqlLiteral::Integer(18))),
        };
        assert!(extract_where_bindings(&expr).is_err());
    }
}
