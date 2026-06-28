//! SSTable leaf-predicate evaluation for the SELECT executor.
//!
//! [`evaluate_leaf`] and [`evaluate_predicates`] are part of the public surface
//! (re-exported via `query::mod`) so the Arrow Flight server applies identical
//! predicate-pushdown semantics. [`validate_token_predicates`] enforces that a
//! `token(...)` restriction names the full partition key in declared order.

use super::super::result::QueryRow;
use super::super::select_optimizer::SSTablePredicate;
use super::value_ops::{compare_values_ordering, values_equal};
use crate::{types::Value, Error, Result};

/// Three-valued (SQL Kleene) outcome of evaluating a single leaf predicate
/// against one row.
///
/// `Unknown` is produced when the predicate references a column that is absent
/// from the row or whose value is `Null` — i.e. a SQL `NULL` operand. Callers
/// that only need pure-`AND` rejection (the historical [`evaluate_predicates`])
/// treat `Unknown` and `False` identically; callers that evaluate `OR`/`NOT`
/// (the Flight nested-predicate evaluator, issue #834) must distinguish them to
/// match SQL `WHERE` semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeafOutcome {
    /// The predicate is definitely satisfied.
    True,
    /// The predicate is definitely not satisfied.
    False,
    /// The predicate's column is missing or `NULL` (SQL `UNKNOWN`).
    Unknown,
}

/// Evaluate a single SSTable leaf predicate against one `QueryRow` with SQL
/// three-valued (Kleene) semantics.
///
/// Returns [`LeafOutcome::Unknown`] when the predicate's column is absent from
/// the row or its value is `Null`; otherwise a definite `True`/`False` from the
/// typed comparison. This is the finer-grained primitive underlying both
/// [`evaluate_predicates`] (pure AND, where `Unknown` rejects like `False`) and
/// the Flight nested-predicate evaluator (issue #834), so all three paths share
/// one copy of the comparison logic (`values_equal` / `compare_values_ordering`).
///
/// `IN` and `Prefix` follow `evaluate_predicates`: `IN` is membership over the
/// value list, `Prefix` is a text `starts_with`. `Range` (two-bound) is included
/// for completeness even though Flight lowers single bounds to `Gt`/`Lt`/etc.
/// `BloomFilter` is always `True` (checked upstream).
pub fn evaluate_leaf(row: &QueryRow, predicate: &SSTablePredicate) -> LeafOutcome {
    use super::super::select_optimizer::SSTableFilterOp;

    // Issue #955: a `token(pk)` predicate constrains the partition-key token, not
    // a stored column. Compute the Murmur3 token of the row's raw partition key
    // (`row.key`) using the same partitioner the rest of the codebase uses, then
    // compare against the i64 bound. An empty key cannot be hashed meaningfully
    // (a synthesised/aggregate row); treat it as Unknown so it is rejected.
    if predicate.is_token() {
        if row.key.0.is_empty() {
            return LeafOutcome::Unknown;
        }
        let row_token = crate::util::cassandra_murmur3::cassandra_murmur3_token(&row.key.0);
        let Some(Value::BigInt(bound)) = predicate.values.first() else {
            return LeafOutcome::Unknown;
        };
        let matches = match &predicate.operation {
            SSTableFilterOp::Gt => row_token > *bound,
            SSTableFilterOp::Gte => row_token >= *bound,
            SSTableFilterOp::Lt => row_token < *bound,
            SSTableFilterOp::Lte => row_token <= *bound,
            SSTableFilterOp::Equal => row_token == *bound,
            // token() only produces inequality/equality predicates upstream.
            _ => return LeafOutcome::Unknown,
        };
        return if matches {
            LeafOutcome::True
        } else {
            LeafOutcome::False
        };
    }

    let column_value = match row.values.get(&predicate.column) {
        // A SQL `NULL` operand (absent column or explicit `Null`) is `UNKNOWN`.
        None | Some(Value::Null) => return LeafOutcome::Unknown,
        Some(v) => v,
    };
    let matches = match &predicate.operation {
        SSTableFilterOp::Equal => predicate
            .values
            .first()
            .is_some_and(|v| values_equal(column_value, v)),
        // Membership uses the same coercing equality as `Equal` so a pushed-down
        // `IN` operand that lowers to a wider numeric type (e.g. `Integer`) still
        // matches a narrow column value (`TinyInt`/`SmallInt`/`Float32`).
        SSTableFilterOp::In => predicate
            .values
            .iter()
            .any(|v| values_equal(column_value, v)),
        SSTableFilterOp::Range => {
            if predicate.values.len() < 2 {
                false
            } else {
                let lo = &predicate.values[0];
                let hi = &predicate.values[1];
                compare_values_ordering(column_value, lo).is_ge()
                    && compare_values_ordering(column_value, hi).is_le()
            }
        }
        // Single-bound clustering inequalities (Issue #788). A missing bound
        // rejects the row, mirroring the `Range` len-guard above.
        SSTableFilterOp::Gt => predicate
            .values
            .first()
            .is_some_and(|b| compare_values_ordering(column_value, b).is_gt()),
        SSTableFilterOp::Gte => predicate
            .values
            .first()
            .is_some_and(|b| compare_values_ordering(column_value, b).is_ge()),
        SSTableFilterOp::Lt => predicate
            .values
            .first()
            .is_some_and(|b| compare_values_ordering(column_value, b).is_lt()),
        SSTableFilterOp::Lte => predicate
            .values
            .first()
            .is_some_and(|b| compare_values_ordering(column_value, b).is_le()),
        SSTableFilterOp::Prefix => matches!(
            (column_value, predicate.values.first()),
            (Value::Text(s), Some(Value::Text(p))) if s.starts_with(p)
        ),
        SSTableFilterOp::BloomFilter => true, // already checked upstream
    };
    if matches {
        LeafOutcome::True
    } else {
        LeafOutcome::False
    }
}

/// Evaluate the SSTable predicate set against a single `QueryRow`.
///
/// Returns `Ok(true)` only if every predicate is satisfied. A missing column
/// causes the row to be rejected.
///
/// Exposed publicly so the Arrow Flight server can apply identical predicate
/// pushdown semantics to its merged rows (output parity with SELECT).
///
/// Implemented in terms of [`evaluate_leaf`]: under pure `AND`, both
/// [`LeafOutcome::False`] and [`LeafOutcome::Unknown`] reject the row, so this
/// preserves the historical "missing column → false" behaviour exactly.
pub fn evaluate_predicates(row: &QueryRow, predicates: &[SSTablePredicate]) -> Result<bool> {
    for predicate in predicates {
        if evaluate_leaf(row, predicate) != LeafOutcome::True {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Validate that every `token(...)` predicate's argument columns match the
/// table's FULL partition-key column list, in declared order (Issue #955
/// follow-up, FINDING 2).
///
/// `evaluate_leaf` evaluates a token predicate by hashing the row's *raw
/// partition key* — it does NOT recompute a token over the columns named in the
/// predicate. So a `token(col, ...)` whose columns are not exactly the partition
/// key (a non-pk column, a strict subset, or a reordering) would be evaluated
/// against the real partition-key token, silently returning rows for a different
/// expression than the user wrote.
///
/// Cassandra rejects `token()` on anything other than the full partition key in
/// declared order, so we do the same: reject with a clear error rather than
/// trust `token_columns`. With no schema we cannot know the partition key, so we
/// also reject (we cannot prove the columns are correct).
pub(super) fn validate_token_predicates(
    predicates: &[SSTablePredicate],
    schema: Option<&crate::schema::TableSchema>,
) -> Result<()> {
    let token_predicates: Vec<&SSTablePredicate> =
        predicates.iter().filter(|p| p.is_token()).collect();
    if token_predicates.is_empty() {
        return Ok(());
    }

    let Some(schema) = schema else {
        return Err(Error::query_execution(
            "token() restriction requires a known table schema to validate its argument \
             against the partition key"
                .to_string(),
        ));
    };

    // The partition key in declared order (positions are 0-based and dense).
    let mut pk_cols: Vec<&crate::schema::KeyColumn> = schema.partition_keys.iter().collect();
    pk_cols.sort_by_key(|c| c.position);
    let expected: Vec<&str> = pk_cols.iter().map(|c| c.name.as_str()).collect();

    for predicate in token_predicates {
        let cols = predicate.token_columns.as_deref().unwrap_or(&[]);
        let matches = cols.len() == expected.len()
            && cols
                .iter()
                .zip(expected.iter())
                .all(|(got, want)| got == want);
        if !matches {
            return Err(Error::query_execution(format!(
                "token() must be applied to the entire partition key in declared order \
                 ({}); got token({})",
                expected.join(", "),
                cols.join(", "),
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::super::select_optimizer::{SSTableFilterOp, SSTablePredicate};
    use super::super::test_support::{composite_pk_schema, row_with_int, row_with_key, single_pk_schema};
    use super::*;
    use crate::types::RowKey;

    /// Issue #955: `evaluate_leaf` on a token predicate hashes the row's raw
    /// partition key with the canonical Murmur3 partitioner and compares against
    /// the i64 bound — matching Cassandra's token inclusivity (`>` exclusive,
    /// `>=` inclusive, etc.).
    #[test]
    fn evaluate_leaf_token_predicate_filters_by_token() {
        let key = b"some-partition-key".to_vec();
        let token = crate::util::cassandra_murmur3::cassandra_murmur3_token(&key);
        let row = row_with_key(&key);

        // token >= exact token → included; token > exact token → excluded.
        let gte = SSTablePredicate::token(
            vec!["id".to_string()],
            SSTableFilterOp::Gte,
            vec![Value::BigInt(token)],
        );
        assert_eq!(evaluate_leaf(&row, &gte), LeafOutcome::True);

        let gt = SSTablePredicate::token(
            vec!["id".to_string()],
            SSTableFilterOp::Gt,
            vec![Value::BigInt(token)],
        );
        assert_eq!(evaluate_leaf(&row, &gt), LeafOutcome::False);

        // A bound above the row's token excludes it under `>=`.
        let gte_above = SSTablePredicate::token(
            vec!["id".to_string()],
            SSTableFilterOp::Gte,
            vec![Value::BigInt(token.saturating_add(1))],
        );
        assert_eq!(evaluate_leaf(&row, &gte_above), LeafOutcome::False);

        // An empty key (synthesised row) is Unknown, never spuriously matched.
        let empty = row_with_key(&[]);
        assert_eq!(evaluate_leaf(&empty, &gte), LeafOutcome::Unknown);
    }

    /// Issue #788: each clustering-key inequality op must include/exclude rows on
    /// the correct side of its bound when evaluated post-scan.
    #[test]
    fn inequality_predicates_apply_single_bound() {
        let bound =
            |op: SSTableFilterOp| SSTablePredicate::column("ck", op, vec![Value::Integer(200)]);
        let eval = |op: SSTableFilterOp, ck: i64| {
            evaluate_predicates(&row_with_int("ck", ck), std::slice::from_ref(&bound(op))).unwrap()
        };

        // ck > 200
        assert!(!eval(SSTableFilterOp::Gt, 200));
        assert!(eval(SSTableFilterOp::Gt, 201));
        // ck >= 200
        assert!(eval(SSTableFilterOp::Gte, 200));
        assert!(!eval(SSTableFilterOp::Gte, 199));
        // ck < 200
        assert!(eval(SSTableFilterOp::Lt, 199));
        assert!(!eval(SSTableFilterOp::Lt, 200));
        // ck <= 200
        assert!(eval(SSTableFilterOp::Lte, 200));
        assert!(!eval(SSTableFilterOp::Lte, 201));
    }

    /// Issue #834: `evaluate_leaf` distinguishes the three SQL truth values.
    /// A present, comparable value yields True/False; an absent column or an
    /// explicit `Null` value yields Unknown so OR/NOT callers get SQL semantics.
    #[test]
    fn evaluate_leaf_is_three_valued() {
        let gt200 = SSTablePredicate::column("ck", SSTableFilterOp::Gt, vec![Value::Integer(200)]);

        // Present value → definite True/False.
        assert_eq!(
            evaluate_leaf(&row_with_int("ck", 201), &gt200),
            LeafOutcome::True
        );
        assert_eq!(
            evaluate_leaf(&row_with_int("ck", 200), &gt200),
            LeafOutcome::False
        );

        // Absent column → Unknown (not False).
        assert_eq!(
            evaluate_leaf(&row_with_int("other", 999), &gt200),
            LeafOutcome::Unknown
        );

        // Explicit Null value → Unknown.
        let mut values = std::collections::HashMap::new();
        values.insert("ck".to_string(), Value::Null);
        let null_row = QueryRow {
            values,
            key: RowKey::new(Vec::new()),
            metadata: Default::default(),
            cell_metadata: None,
        };
        assert_eq!(evaluate_leaf(&null_row, &gt200), LeafOutcome::Unknown);
    }

    /// Pushed-down `IN` operands lower to wide numeric types (`Integer`), but
    /// the row value for a CQL `tinyint`/`smallint`/`float` column is a narrow
    /// variant. Membership must coerce (like `Equal`) so the match still holds.
    #[test]
    fn evaluate_leaf_in_coerces_narrow_numeric_columns() {
        // Operands as they arrive from a Flight ticket (wide types).
        let in_pred = SSTablePredicate::column(
            "v",
            SSTableFilterOp::In,
            vec![Value::Integer(7), Value::Integer(9)],
        );

        let row_of = |val: Value| {
            let mut values = std::collections::HashMap::new();
            values.insert("v".to_string(), val);
            QueryRow {
                values,
                key: RowKey::new(Vec::new()),
                metadata: Default::default(),
                cell_metadata: None,
            }
        };

        // Narrow column values must still match the wide IN operands.
        assert_eq!(
            evaluate_leaf(&row_of(Value::TinyInt(7)), &in_pred),
            LeafOutcome::True
        );
        assert_eq!(
            evaluate_leaf(&row_of(Value::SmallInt(9)), &in_pred),
            LeafOutcome::True
        );
        assert_eq!(
            evaluate_leaf(&row_of(Value::Float32(7.0)), &in_pred),
            LeafOutcome::True
        );
        // A non-member narrow value is still False (not Unknown).
        assert_eq!(
            evaluate_leaf(&row_of(Value::TinyInt(8)), &in_pred),
            LeafOutcome::False
        );
    }

    /// Issue #788: the `pk = ? AND ck >= 0 AND ck < 200` shape — a two-bound AND
    /// set — must include `[0, 199]` and exclude `200`/`1000`, reproducing the
    /// 200-row slice the issue expects (previously the whole partition leaked).
    #[test]
    fn two_bound_inequality_slice_selects_half_open_range() {
        let predicates = vec![
            SSTablePredicate::column("ck", SSTableFilterOp::Gte, vec![Value::Integer(0)]),
            SSTablePredicate::column("ck", SSTableFilterOp::Lt, vec![Value::Integer(200)]),
        ];
        let in_slice = |ck: i64| evaluate_predicates(&row_with_int("ck", ck), &predicates).unwrap();

        assert!(in_slice(0), "lower bound is inclusive");
        assert!(in_slice(199), "last row in [0, 200) is included");
        assert!(!in_slice(200), "upper bound is exclusive");
        assert!(!in_slice(1000), "rows past the slice are excluded");
        assert!(!in_slice(-1), "rows below the slice are excluded");
    }

    /// FINDING 2: a `token(...)` over the FULL partition key in declared order is
    /// accepted (validation passes), so the existing token fast-path/evaluation
    /// behaviour is preserved.
    #[test]
    fn validate_token_predicates_accepts_full_key_in_order() {
        let schema = composite_pk_schema(("a", "int"), ("b", "int"));
        let pred = SSTablePredicate::token(
            vec!["a".to_string(), "b".to_string()],
            SSTableFilterOp::Gt,
            vec![Value::BigInt(0)],
        );
        assert!(
            validate_token_predicates(std::slice::from_ref(&pred), Some(&schema)).is_ok(),
            "token(a, b) over the full partition key in declared order must be accepted",
        );

        // Single-column key: token(id) is the full key.
        let single = single_pk_schema("id", "int");
        let pred_single = SSTablePredicate::token(
            vec!["id".to_string()],
            SSTableFilterOp::Gte,
            vec![Value::BigInt(0)],
        );
        assert!(
            validate_token_predicates(std::slice::from_ref(&pred_single), Some(&single)).is_ok(),
            "token(id) over a single-column partition key must be accepted",
        );
    }

    /// FINDING 2: `token(non_pk_col)` must be REJECTED — `evaluate_leaf` would
    /// otherwise hash the real partition key and silently return rows for a
    /// different expression than the user wrote.
    #[test]
    fn validate_token_predicates_rejects_non_pk_column() {
        let schema = single_pk_schema("id", "int");
        let pred = SSTablePredicate::token(
            vec!["not_the_pk".to_string()],
            SSTableFilterOp::Gt,
            vec![Value::BigInt(0)],
        );
        assert!(
            validate_token_predicates(std::slice::from_ref(&pred), Some(&schema)).is_err(),
            "token(non_pk_col) must be rejected, not evaluated against the real pk token",
        );
    }

    /// FINDING 2: `token(b, a)` on a `(a, b)` key — right columns, WRONG order —
    /// must be rejected (Cassandra requires declared order).
    #[test]
    fn validate_token_predicates_rejects_reordered_composite() {
        let schema = composite_pk_schema(("a", "int"), ("b", "int"));
        let reordered = SSTablePredicate::token(
            vec!["b".to_string(), "a".to_string()],
            SSTableFilterOp::Lt,
            vec![Value::BigInt(0)],
        );
        assert!(
            validate_token_predicates(std::slice::from_ref(&reordered), Some(&schema)).is_err(),
            "token(b, a) on a (a, b) key must be rejected (wrong order)",
        );

        // A strict subset (missing the second column) is also rejected.
        let subset = SSTablePredicate::token(
            vec!["a".to_string()],
            SSTableFilterOp::Lt,
            vec![Value::BigInt(0)],
        );
        assert!(
            validate_token_predicates(std::slice::from_ref(&subset), Some(&schema)).is_err(),
            "token(a) on a (a, b) key must be rejected (partial key)",
        );
    }

    /// FINDING 2: with no schema we cannot prove the token columns are the
    /// partition key, so any token predicate is rejected (never trusted).
    #[test]
    fn validate_token_predicates_rejects_without_schema() {
        let pred = SSTablePredicate::token(
            vec!["id".to_string()],
            SSTableFilterOp::Gt,
            vec![Value::BigInt(0)],
        );
        assert!(
            validate_token_predicates(std::slice::from_ref(&pred), None).is_err(),
            "a token predicate with no schema must be rejected",
        );
        // Ordinary column predicates are unaffected by token validation.
        let col = SSTablePredicate::column("id", SSTableFilterOp::Equal, vec![Value::Integer(1)]);
        assert!(
            validate_token_predicates(std::slice::from_ref(&col), None).is_ok(),
            "non-token predicates must pass token validation even without a schema",
        );
    }
}
