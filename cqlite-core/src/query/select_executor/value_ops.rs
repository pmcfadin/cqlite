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
    if same_numeric_family(a, b) {
        if let (Some(x), Some(y)) = (a.as_f64(), b.as_f64()) {
            // Cassandra/Java `Double.compare` total order: NaN last, -0.0 < +0.0
            // (issues #1870, #2010). Never `partial_cmp().unwrap_or(Equal)`,
            // which collapses NaN and signed zeros to Equal.
            return Ok(crate::float_cmp::cassandra_double_cmp(x, y));
        }
    }
    if std::mem::discriminant(a) == std::mem::discriminant(b) {
        return a.partial_cmp(b).ok_or_else(|| {
            Error::query_execution("Cannot compare incompatible types".to_string())
        });
    }
    // Data-safety (issue #1694): log the operand TYPES, never their values.
    tracing::debug!(
        "Cannot compare values of incompatible types: {:?} vs {:?}",
        a.data_type(),
        b.data_type()
    );
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

    /// The query ordering comparator (used by ORDER BY / MIN / MAX) must match
    /// Cassandra/Java `Double.compare`: NaN last, -0.0 < +0.0 (issues #1870/#2010).
    #[test]
    fn compare_values_ordering_double_matches_cassandra() {
        use std::cmp::Ordering;
        let f = Value::Float; // f64 → CQL `double`
        assert_eq!(
            compare_values_ordering(&f(f64::NAN), &f(f64::INFINITY)),
            Ordering::Greater,
            "NaN sorts after +Infinity"
        );
        assert_eq!(
            compare_values_ordering(&f(f64::NAN), &f(f64::NAN)),
            Ordering::Equal,
            "two NaNs compare equal"
        );
        assert_eq!(
            compare_values_ordering(&f(-0.0), &f(0.0)),
            Ordering::Less,
            "-0.0 < +0.0"
        );
        assert_eq!(compare_values_ordering(&f(1.0), &f(2.0)), Ordering::Less);
    }

    /// Sorting `Value::Float` keys yields the Cassandra oracle order
    /// `[-Inf, -0.0, +0.0, 1.0, +Inf, NaN, NaN]`.
    #[test]
    fn order_by_double_sort_matches_oracle() {
        let mut v = vec![
            Value::Float(1.0),
            Value::Float(f64::NAN),
            Value::Float(-0.0),
            Value::Float(0.0),
            Value::Float(f64::NEG_INFINITY),
            Value::Float(f64::INFINITY),
            Value::Float(f64::NAN),
        ];
        v.sort_by(compare_values_ordering);
        let f = |i: usize| match v[i] {
            Value::Float(x) => x,
            _ => unreachable!(),
        };
        assert_eq!(f(0), f64::NEG_INFINITY);
        assert!(f(1) == 0.0 && f(1).is_sign_negative(), "index 1 = -0.0");
        assert!(f(2) == 0.0 && f(2).is_sign_positive(), "index 2 = +0.0");
        assert_eq!(f(3), 1.0);
        assert_eq!(f(4), f64::INFINITY);
        assert!(f(5).is_nan() && f(6).is_nan(), "NaNs sort last");
    }

    /// `Value::Float32` (CQL `float`) shares the same ordering semantics.
    #[test]
    fn order_by_float32_sort_matches_oracle() {
        let mut v = [
            Value::Float32(f32::NAN),
            Value::Float32(0.0),
            Value::Float32(-0.0),
            Value::Float32(f32::INFINITY),
        ];
        v.sort_by(compare_values_ordering);
        let f = |i: usize| match v[i] {
            Value::Float32(x) => x,
            _ => unreachable!(),
        };
        assert!(f(0) == 0.0 && f(0).is_sign_negative(), "index 0 = -0.0");
        assert!(f(1) == 0.0 && f(1).is_sign_positive(), "index 1 = +0.0");
        assert_eq!(f(2), f32::INFINITY);
        assert!(f(3).is_nan(), "NaN sorts last");
    }

    /// MIN/MAX (aggregation uses `compare_values_ordering`): MIN over signed
    /// zeros selects -0.0; NaN never wins MIN but is the MAX.
    #[test]
    fn min_max_double_matches_cassandra() {
        let data = [
            Value::Float(f64::NAN),
            Value::Float(3.0),
            Value::Float(-0.0),
            Value::Float(0.0),
            Value::Float(-2.0),
        ];
        let min = data
            .iter()
            .min_by(|a, b| compare_values_ordering(a, b))
            .unwrap();
        assert!(matches!(min, Value::Float(x) if *x == -2.0), "MIN = -2.0");

        let max = data
            .iter()
            .max_by(|a, b| compare_values_ordering(a, b))
            .unwrap();
        assert!(
            matches!(max, Value::Float(x) if x.is_nan()),
            "MAX = NaN (sorts last)"
        );

        // MIN over just the signed zeros picks -0.0.
        let zeros = [Value::Float(0.0), Value::Float(-0.0)];
        let zmin = zeros
            .iter()
            .min_by(|a, b| compare_values_ordering(a, b))
            .unwrap();
        assert!(
            matches!(zmin, Value::Float(x) if *x == 0.0 && x.is_sign_negative()),
            "MIN of {{-0.0, +0.0}} = -0.0"
        );
    }
}
