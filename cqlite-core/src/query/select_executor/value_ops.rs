//! Value comparison and arithmetic helpers for the SELECT executor.
//!
//! Pure functions over [`Value`] shared by predicate evaluation, sorting,
//! aggregation, and constant folding. They were previously inline in
//! `select_executor.rs`; centralising them keeps one copy of the comparison and
//! arithmetic semantics across every execution path.

use super::super::select_ast::ArithmeticOperator;
use crate::{types::Value, Error, Result};

/// Compare two `Value`s for equality, including limited cross-type numeric
/// coercion (int↔bigint, int↔float, bigint↔float).
///
/// `Value` implements `PartialEq` natively but only matches identical variants;
/// we additionally treat the small set of cross-numeric cases that show up in
/// CQL predicates.
pub(super) fn values_equal(a: &Value, b: &Value) -> bool {
    if a == b {
        return true;
    }
    // Only coerce when both operands are numeric — otherwise non-numeric
    // pairs (e.g. Text vs Integer) would spuriously compare equal via `as_f64`.
    if same_numeric_family(a, b) {
        if let (Some(x), Some(y)) = (a.as_f64(), b.as_f64()) {
            return x == y;
        }
    }
    false
}

/// True when both `Value`s are numeric variants eligible for cross-type coercion.
pub(super) fn same_numeric_family(a: &Value, b: &Value) -> bool {
    a.as_f64().is_some() && b.as_f64().is_some()
}

/// Compare two `Value`s for ordering, returning `Ordering::Equal` for
/// incomparable variants. Used by sorting/aggregation paths that historically
/// swallowed comparison errors via `unwrap_or(0)`.
pub(super) fn compare_values_ordering(a: &Value, b: &Value) -> std::cmp::Ordering {
    try_compare_values(a, b).unwrap_or(std::cmp::Ordering::Equal)
}

/// Compare two `Value`s for ordering, returning an error when the operand
/// types are not comparable. Preferred in WHERE-clause evaluation so users see
/// a real diagnostic rather than a silent equality.
///
/// Cross-type numerics are coerced via `f64` first; same-variant comparisons
/// fall back to `Value::partial_cmp`. We deliberately avoid `partial_cmp` for
/// non-matching variants because it stringifies and would produce surprising
/// orderings (e.g. `Text("9")` < `Text("10")` lexicographically).
pub(super) fn try_compare_values(a: &Value, b: &Value) -> Result<std::cmp::Ordering> {
    use std::cmp::Ordering;
    if same_numeric_family(a, b) {
        if let (Some(x), Some(y)) = (a.as_f64(), b.as_f64()) {
            return Ok(x.partial_cmp(&y).unwrap_or(Ordering::Equal));
        }
    }
    if std::mem::discriminant(a) == std::mem::discriminant(b) {
        return a.partial_cmp(b).ok_or_else(|| {
            Error::query_execution("Cannot compare incompatible types".to_string())
        });
    }
    log::debug!("Cannot compare {:?} with {:?}", a, b);
    Err(Error::query_execution(
        "Cannot compare incompatible types".to_string(),
    ))
}

/// Apply an `ArithmeticOperator` to two same-typed numeric `Value`s.
///
/// Behaviour matches the previous inline implementations: same-type only
/// (no implicit coercion), and division/modulo by zero are reported as
/// query-execution errors. Float division-by-zero (matching the original
/// runtime path) yields IEEE inf/NaN rather than an error.
pub(super) fn eval_arithmetic(op: &ArithmeticOperator, left: Value, right: Value) -> Result<Value> {
    use ArithmeticOperator::*;
    macro_rules! int_op {
        ($a:expr, $b:expr, $ctor:expr) => {
            match op {
                Add => Ok($ctor($a + $b)),
                Subtract => Ok($ctor($a - $b)),
                Multiply => Ok($ctor($a * $b)),
                Divide => {
                    if $b == 0 {
                        Err(Error::query_execution("Division by zero".to_string()))
                    } else {
                        Ok($ctor($a / $b))
                    }
                }
                Modulo => {
                    if $b == 0 {
                        Err(Error::query_execution("Modulo by zero".to_string()))
                    } else {
                        Ok($ctor($a % $b))
                    }
                }
            }
        };
    }
    match (left, right) {
        (Value::Integer(a), Value::Integer(b)) => int_op!(a, b, Value::Integer),
        (Value::BigInt(a), Value::BigInt(b)) => int_op!(a, b, Value::BigInt),
        (Value::Float(a), Value::Float(b)) => match op {
            Add => Ok(Value::Float(a + b)),
            Subtract => Ok(Value::Float(a - b)),
            Multiply => Ok(Value::Float(a * b)),
            Divide => Ok(Value::Float(a / b)),
            Modulo => Ok(Value::Float(a % b)),
        },
        _ => Err(Error::query_execution(
            "Incompatible types for arithmetic".to_string(),
        )),
    }
}

/// Constant-folding arithmetic. Same operand-type rules as `eval_arithmetic`,
/// plus BigInt support and per-operator error wording matching the legacy
/// implementation (e.g. `"Cannot add incompatible types"` and
/// `"Modulo only supported for integers"`).
pub(super) fn const_arithmetic(
    op: &ArithmeticOperator,
    left: Value,
    right: Value,
) -> Result<Value> {
    use ArithmeticOperator::*;

    // Modulo's error wording is special: any non-integer combination must
    // report `"Modulo only supported for integers"` regardless of which side
    // is offending.
    if matches!(op, Modulo) {
        return match (left, right) {
            (Value::Integer(a), Value::Integer(b)) => {
                eval_arithmetic(op, Value::Integer(a), Value::Integer(b))
            }
            (Value::BigInt(a), Value::BigInt(b)) => {
                eval_arithmetic(op, Value::BigInt(a), Value::BigInt(b))
            }
            _ => Err(Error::query_execution(
                "Modulo only supported for integers".to_string(),
            )),
        };
    }

    let verb = match op {
        Add => "add",
        Subtract => "subtract",
        Multiply => "multiply",
        Divide => "divide",
        Modulo => unreachable!("handled above"),
    };

    match (left, right) {
        (Value::Integer(a), Value::Integer(b)) => {
            eval_arithmetic(op, Value::Integer(a), Value::Integer(b))
        }
        (Value::BigInt(a), Value::BigInt(b)) => {
            eval_arithmetic(op, Value::BigInt(a), Value::BigInt(b))
        }
        (Value::Float(a), Value::Float(b)) => {
            // Constant Float Divide rejects 0.0 (legacy behaviour); runtime
            // Float divide does not. Modulo on Float is rejected above.
            if matches!(op, Divide) && b == 0.0 {
                return Err(Error::query_execution("Division by zero".to_string()));
            }
            eval_arithmetic(op, Value::Float(a), Value::Float(b))
        }
        _ => Err(Error::query_execution(format!(
            "Cannot {} incompatible types",
            verb
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_comparison() {
        use std::cmp::Ordering;
        assert_eq!(
            try_compare_values(&Value::Integer(5), &Value::Integer(3)).unwrap(),
            Ordering::Greater
        );
        assert_eq!(
            try_compare_values(&Value::Integer(3), &Value::Integer(5)).unwrap(),
            Ordering::Less
        );
        assert_eq!(
            try_compare_values(&Value::Integer(5), &Value::Integer(5)).unwrap(),
            Ordering::Equal
        );
    }
}
