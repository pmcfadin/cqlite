//! Value comparison and arithmetic helpers for the SELECT executor.
//!
//! Pure functions over [`Value`] shared by predicate evaluation, sorting,
//! aggregation, and constant folding. They were previously inline in
//! `select_executor.rs`; centralising them keeps one copy of the comparison and
//! arithmetic semantics across every execution path.

use super::super::select_ast::{ArithmeticOperator, ComparisonOperator};
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
    // Integer-vs-integer must compare as the widest native integer (`i128`),
    // NEVER via `as_f64` (issue #2231): two distinct `i64` above 2^53 collapse
    // to the same `f64` mantissa, so an f64 fallback would report them equal and
    // — because a fully-translated Trino conjunct is removed from the plan — leak
    // rows Trino would drop. Every CQL integral type fits losslessly in `i128`.
    if let (Some(x), Some(y)) = (as_integral_i128(a), as_integral_i128(b)) {
        return x == y;
    }
    // Otherwise coerce only when both operands are numeric — a genuine float on
    // at least one side. Non-numeric pairs (e.g. Text vs Integer) must not
    // spuriously compare equal via `as_f64`.
    if same_numeric_family(a, b) {
        if let (Some(x), Some(y)) = (a.as_f64(), b.as_f64()) {
            return x == y;
        }
    }
    false
}

/// The exact integer value of an integral CQL numeric `Value`, or `None` for
/// float/non-numeric variants. Widened to `i128` so every integral type
/// (`tinyint`..`bigint`, `counter`) is represented losslessly, enabling exact
/// integer equality/comparison without an `f64` round-trip (issue #2231).
pub(super) fn as_integral_i128(v: &Value) -> Option<i128> {
    match v {
        Value::Integer(i) => Some(*i as i128),
        Value::BigInt(i) => Some(*i as i128),
        Value::Counter(i) => Some(*i as i128),
        Value::TinyInt(i) => Some(*i as i128),
        Value::SmallInt(i) => Some(*i as i128),
        _ => None,
    }
}

/// True when `v` is an IEEE floating-point value that is NaN (CQL `float`/
/// `double`). Predicate (WHERE) evaluation uses this to implement SQL
/// three-valued logic: any relational comparison (`<`, `<=`, `>`, `>=`) with a
/// NaN operand is UNKNOWN, so the row is dropped (issue #2231). Only the `float`
/// variants can be NaN — integral types never are, so this is principled, not a
/// bit-pattern heuristic.
pub(in crate::query) fn is_nan_value(v: &Value) -> bool {
    match v {
        Value::Float(x) => x.is_nan(),
        Value::Float32(x) => x.is_nan(),
        _ => false,
    }
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

/// Does the empty-buffer sentinel `tag` name the declared type of the NON-EMPTY
/// value `other` — i.e. are the two the same CQL type, so that "empty sorts
/// before non-empty" applies (issue #3805)?
///
/// # `Value::data_type()` IS LOSSY FOR uuid/timeuuid, AND THAT IS THE WHOLE
/// REASON THIS FUNCTION EXISTS
///
/// CQLite stores BOTH CQL `uuid` and CQL `timeuuid` as `Value::Uuid([u8; 16])`,
/// and `data_type()` answers `CqlType::Uuid` for both. So a plain
/// `for_cql_type(&other.data_type()) == Some(tag)` REJECTED
/// `Empty(TimeUuid)` against every non-empty timeuuid — an ordering that
/// refuses a legitimate pair (roborev job 438 F2).
///
/// The 16-byte pair is therefore admitted **for the EMPTY-vs-NON-EMPTY case
/// only**, which is sound because of what is being asserted: the answer for
/// such a pair is `Less` (empty first) **whichever** of the two types it is —
/// `db/marshal/Int32Type.java:61-71` at `cassandra-5.0.8` returns
/// `Boolean.compare(right.isEmpty, left.isEmpty)` as soon as EITHER side is
/// empty, and `TimeUUIDType`/`UUIDType` both inherit that empty short-circuit,
/// so the type-specific logic below it is never reached. Two things this
/// deliberately does NOT do:
///
/// * it does NOT conflate `uuid` and `timeuuid` in any NON-EMPTY comparison —
///   those really do differ (`TimeUUIDType` orders by embedded timestamp
///   first), and this function is only ever consulted when one side IS a
///   sentinel, so no non-empty ordering can reach it;
/// * it does NOT change `Value` to preserve uuid/timeuuid identity — that is a
///   second public-surface change nobody ruled.
///
/// # This is the ONLY lossy pair among the admitted families
///
/// Checked family by family against `Value::data_type()`: `int`→`Integer`,
/// `bigint`→`BigInt`, `counter`→`Counter`, `float`→`Float32`,
/// `double`→`Float`, `timestamp`→`Timestamp`, `boolean`→`Boolean`,
/// `inet`→`Inet`, `varint`→`Varint`, `decimal`→`Decimal` — each a distinct
/// `Value` variant mapping to a distinct `CqlType`. Only `uuid`/`timeuuid`
/// share one variant. (`text`/`ascii`/`varchar` also share `Value::Text`, but
/// none of them is an admitted family — an empty buffer is a MEANINGFUL value
/// there, so no sentinel names them; see `EmptyValueType::for_cql_type`.)
fn empty_tag_matches_operand(tag: crate::types::EmptyValueType, other: &Value) -> bool {
    use crate::types::EmptyValueType as E;
    match crate::types::EmptyValueType::for_cql_type(&other.data_type()) {
        Some(observed) if observed == tag => true,
        // The uuid/timeuuid pair, in both directions: `Value::Uuid` observes as
        // `Uuid`, so an `Empty(TimeUuid)` tag must still match it, and an
        // `Empty(Uuid)` tag must still match a value a caller built as a
        // timeuuid (also `Value::Uuid`).
        Some(observed) => matches!(
            (tag, observed),
            (E::Uuid | E::TimeUuid, E::Uuid | E::TimeUuid)
        ),
        None => false,
    }
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
    // SENTINEL vs SENTINEL — HANDLED HERE, ABOVE the discriminant test, and it
    // must stay above it (issue #3805; roborev job 451 + lead audit Q3). This
    // one arm owns BOTH halves of that pair's contract, because every
    // `Value::Empty` shares ONE discriminant whatever its tag, so neither the
    // discriminant test below nor the sentinel-vs-non-empty arms under it can
    // decide the case correctly:
    //
    // * DIFFERENT tags are DIFFERENT DECLARED TYPES, and this function's rule
    //   for those is an ERROR. Left to the discriminant test, the pair reached
    //   `Value::partial_cmp`, which orders two sentinels BY TAG — it must, being
    //   a total order (`types::value_ord`) — so a cross-type `WHERE` comparison
    //   got an ORDERING instead of the type diagnostic. Nothing lossy happens
    //   here: a sentinel CARRIES its type, so `uuid` vs `timeuuid` is refused
    //   too, and `empty_tag_matches_operand`'s admission of that pair — which
    //   exists only because a NON-EMPTY timeuuid is indistinguishable from a
    //   uuid in `Value` — deliberately does not apply.
    // * EQUAL tags are the same (empty) bytes ⇒ `Equal`, exactly as
    //   `Int32Type.compareCustom` reports for two empty buffers
    //   (`db/marshal/Int32Type.java:61-71` at `cassandra-5.0.8`:
    //   `Boolean.compare(true, true) == 0`). This is LOAD-BEARING FOR THE `Ord`
    //   CONTRACT, not a nicety: `compare_values_ordering` feeds `sort_by`, and
    //   without this arm the pair would fall into the `Less`/`Greater` arms
    //   below, whose left-hand `if let Value::Empty(tag) = a` matches and
    //   answers `Less` for a pair that must be `Equal` — breaking reflexivity
    //   and able to panic the sort.
    //
    // So a refactor may NOT move this block below the discriminant test, and
    // may not narrow it to specific tags; anything that reorders these blocks
    // has to keep `(Empty, Empty)` decided before either of them.
    if let (Value::Empty(x), Value::Empty(y)) = (a, b) {
        return if x == y {
            Ok(std::cmp::Ordering::Equal)
        } else {
            // Data-safety (issue #1694): the tags ARE type names, so they are
            // safe to log; the values are not, and there are none here.
            tracing::debug!(
                "Cannot compare empty-buffer sentinels of incompatible types: {:?} vs {:?}",
                x,
                y
            );
            Err(Error::query_execution(
                "Cannot compare incompatible types".to_string(),
            ))
        };
    }
    if std::mem::discriminant(a) == std::mem::discriminant(b) {
        return a.partial_cmp(b).ok_or_else(|| {
            Error::query_execution("Cannot compare incompatible types".to_string())
        });
    }
    // EMPTY-BUFFER SENTINEL vs a NON-EMPTY value OF THE SAME DECLARED TYPE
    // (issue #3805). The discriminants differ — `Value::Empty(Int)` and
    // `Value::Integer(5)` are different variants — but Cassandra compares them
    // with ONE comparator and puts the empty buffer strictly first:
    // `db/marshal/Int32Type.java:61-71` at `cassandra-5.0.8` returns
    // `Boolean.compare(right.isEmpty, left.isEmpty)` whenever EITHER side is
    // empty, i.e. `-1` when only the left is. Without this branch the pair
    // reached the incomparable-types error below, which would drop a legitimate
    // key from an ORDER BY / WHERE evaluation.
    //
    // Comparability is decided by the DECLARED TYPE, never by byte shape
    // (no-heuristics, #28): the sentinel carries its type, and the other side's
    // `data_type()` must map back to that same admitted family. A sentinel
    // against a DIFFERENT type stays incomparable, exactly as two mismatched
    // scalars do.
    if let Value::Empty(tag) = a {
        if empty_tag_matches_operand(*tag, b) {
            return Ok(std::cmp::Ordering::Less);
        }
    }
    if let Value::Empty(tag) = b {
        if empty_tag_matches_operand(*tag, a) {
            return Ok(std::cmp::Ordering::Greater);
        }
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

/// Relational comparison for PREDICATE (WHERE) evaluation, with SQL
/// three-valued logic for IEEE NaN AND exact-integer ordering above the f64
/// precision boundary.
///
/// Returns `Ok(None)` — SQL UNKNOWN, so the caller DROPS the row — when either
/// operand is a NaN float. Cassandra's total order (`cassandra_double_cmp`, used
/// by `try_compare_values`/`compare_values_ordering`) sorts NaN as the GREATEST
/// value, which would make `d > 1.5` TRUE for `d = NaN` and leak rows Trino
/// would drop once the conjunct is pushed down and removed from the plan (issue
/// #2231).
///
/// When both operands are integral (mirroring `values_equal`'s own structure),
/// compares them as exact `i128` BEFORE any f64 fallback: two distinct `i64`
/// above 2^53 collapse to the same f64 mantissa, so `bigcol > 9007199254740992`
/// against a row where `bigcol = 9007199254740993` would otherwise compare
/// `Equal` via `cassandra_double_cmp` and wrongly evaluate `is_gt()` to `false`
/// — the same leak-once-pushed-down mechanism as the `=` divergence.
///
/// This function is for filter/`WHERE` evaluation ONLY — do NOT use it for
/// ORDER BY / MIN / MAX / clustering-key ordering, which must keep the
/// NaN-greatest total order and existing f64-based numeric ordering
/// (`try_compare_values`/`compare_values_ordering`, unchanged).
pub(super) fn try_compare_values_predicate(
    a: &Value,
    b: &Value,
) -> Result<Option<std::cmp::Ordering>> {
    if is_nan_value(a) || is_nan_value(b) {
        return Ok(None);
    }
    if let (Some(x), Some(y)) = (as_integral_i128(a), as_integral_i128(b)) {
        return Ok(Some(x.cmp(&y)));
    }
    try_compare_values(a, b).map(Some)
}

/// `compare_values_ordering` counterpart for predicate evaluation: routes
/// through [`try_compare_values_predicate`] so it shares BOTH predicate-only
/// fixes (issue #2231) — NaN → `None` (SQL UNKNOWN → drop the row) and exact
/// `i128` integer ordering above the f64 precision boundary — then swallows any
/// remaining comparison error to `Ordering::Equal`, matching
/// `compare_values_ordering`'s error-tolerant behaviour for non-NaN,
/// non-integral pairs. Used by the SSTable leaf-predicate evaluator's
/// inequalities so neither a NaN nor a large `bigint` pair is mishandled.
pub(super) fn compare_values_ordering_predicate(
    a: &Value,
    b: &Value,
) -> Option<std::cmp::Ordering> {
    match try_compare_values_predicate(a, b) {
        Ok(ordering) => ordering,
        Err(_) => Some(std::cmp::Ordering::Equal),
    }
}

/// Evaluate a scalar `ComparisonOperator` (`=`, `!=`, `<`, `<=`, `>`, `>=`)
/// over two already-evaluated operands with SQL PREDICATE semantics (issue
/// #2231). Shared by the expression-pushdown WHERE evaluator so equality uses
/// exact integer comparison (`values_equal`, no `f64` collapse above 2^53) and
/// the four inequalities treat a NaN operand as UNKNOWN → `false` (row dropped),
/// never NaN-greatest. Non-scalar operators (`IN`, `LIKE`, `IS [NOT] NULL`, …)
/// have their own branches and are rejected here.
pub(super) fn eval_scalar_comparison(
    op: &ComparisonOperator,
    left: &Value,
    right: &Value,
) -> Result<bool> {
    use ComparisonOperator::*;
    Ok(match op {
        Equal => values_equal(left, right),
        NotEqual => !values_equal(left, right),
        LessThan => try_compare_values_predicate(left, right)?.is_some_and(|o| o.is_lt()),
        LessThanOrEqual => try_compare_values_predicate(left, right)?.is_some_and(|o| o.is_le()),
        GreaterThan => try_compare_values_predicate(left, right)?.is_some_and(|o| o.is_gt()),
        GreaterThanOrEqual => try_compare_values_predicate(left, right)?.is_some_and(|o| o.is_ge()),
        other => {
            return Err(Error::query_execution(format!(
                "operator {:?} is not a scalar comparison",
                other
            )))
        }
    })
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

    /// Issue #2231, divergence 2: `values_equal` distinguishes two DISTINCT
    /// `i64` values straddling the 2^53 f64-precision boundary. An `as_f64`
    /// fallback would round both to the same mantissa and report them equal,
    /// leaking rows once a `bigcol = ...` conjunct is pushed down and removed
    /// from the Trino plan.
    #[test]
    fn values_equal_distinguishes_large_i64_across_f64_boundary() {
        let two53 = 1_i64 << 53; // 9_007_199_254_740_992
        let plus1 = two53 + 1; // 9_007_199_254_740_993 (not representable as f64)
                               // Sanity: the two integers DO collapse to the same f64.
        assert_eq!(
            two53 as f64, plus1 as f64,
            "precondition: f64 collapses them"
        );

        // The exact-integer comparison must keep them distinct.
        assert!(
            !values_equal(&Value::BigInt(two53), &Value::BigInt(plus1)),
            "2^53 != 2^53 + 1 as bigint"
        );
        assert!(
            values_equal(&Value::BigInt(plus1), &Value::BigInt(plus1)),
            "identical large bigints are equal"
        );
        // Repro operand: bigcol = 9_007_199_254_740_993 must NOT match 992.
        assert!(!values_equal(
            &Value::BigInt(9_007_199_254_740_992),
            &Value::BigInt(9_007_199_254_740_993),
        ));

        // Cross-integral-type equality still holds (int == bigint numerically).
        assert!(values_equal(&Value::Integer(7), &Value::BigInt(7)));
        assert!(values_equal(&Value::TinyInt(7), &Value::SmallInt(7)));
        assert!(!values_equal(&Value::Integer(7), &Value::BigInt(8)));
        // Integer-vs-float equality still coerces (genuine float on one side).
        assert!(values_equal(&Value::BigInt(2), &Value::Float(2.0)));
        assert!(!values_equal(&Value::BigInt(2), &Value::Float(2.5)));
    }

    /// Issue #2231 follow-up (roborev blocker): `try_compare_values_predicate` /
    /// `compare_values_ordering_predicate` must ALSO compare large integers
    /// exactly — not just `values_equal`. Without the `as_integral_i128`
    /// short-circuit, `bigcol > 9007199254740992` against a row where
    /// `bigcol = 9007199254740993` would coerce both to the same f64 mantissa,
    /// compare `Equal`, and wrongly evaluate `is_gt()` to `false` (row dropped).
    #[test]
    fn predicate_ordering_distinguishes_large_i64_across_f64_boundary() {
        use std::cmp::Ordering;
        let two53 = 1_i64 << 53; // 9_007_199_254_740_992
        let plus1 = two53 + 1; // 9_007_199_254_740_993

        // ...993 > ...992 must hold exactly (f64 would report Equal).
        assert_eq!(
            try_compare_values_predicate(&Value::BigInt(plus1), &Value::BigInt(two53)).unwrap(),
            Some(Ordering::Greater),
            "9007199254740993 > 9007199254740992 must hold exactly"
        );
        assert_eq!(
            compare_values_ordering_predicate(&Value::BigInt(plus1), &Value::BigInt(two53)),
            Some(Ordering::Greater)
        );
        // The symmetric direction: ...992 is NOT greater than ...993.
        assert_eq!(
            try_compare_values_predicate(&Value::BigInt(two53), &Value::BigInt(plus1)).unwrap(),
            Some(Ordering::Less)
        );
        // Equal large integers still compare Equal.
        assert_eq!(
            try_compare_values_predicate(&Value::BigInt(plus1), &Value::BigInt(plus1)).unwrap(),
            Some(Ordering::Equal)
        );
    }

    /// Issue #2231, divergence 1: under PREDICATE (WHERE) semantics a NaN
    /// operand makes every relational comparison UNKNOWN, so the row is dropped.
    /// Historically only `Lt`/`Lte`/`Eq` dropped NaN while `Gt`/`Gte` leaked it
    /// (NaN sorts greatest under `cassandra_double_cmp`); all four inequalities
    /// must now drop it, and `Eq` continues to.
    #[test]
    fn nan_predicate_comparison_is_unknown_for_all_relations() {
        let nan = Value::Float(f64::NAN);
        let bound = Value::Float(1.5);

        // `d > 1.5` / `d >= 1.5` with d = NaN: previously TRUE (leak), now dropped.
        let cmp = try_compare_values_predicate(&nan, &bound).unwrap();
        assert!(cmp.is_none(), "NaN vs 1.5 is UNKNOWN (Gt/Gte)");
        assert!(
            !cmp.is_some_and(|o| o.is_gt()),
            "d > 1.5 with NaN is dropped"
        );
        assert!(
            !cmp.is_some_and(|o| o.is_ge()),
            "d >= 1.5 with NaN is dropped"
        );
        // Lt/Lte were already consistent (dropped) — confirm they stay dropped.
        assert!(
            !cmp.is_some_and(|o| o.is_lt()),
            "d < 1.5 with NaN is dropped"
        );
        assert!(
            !cmp.is_some_and(|o| o.is_le()),
            "d <= 1.5 with NaN is dropped"
        );
        // NaN on the right-hand side is symmetric.
        assert!(try_compare_values_predicate(&bound, &nan)
            .unwrap()
            .is_none());
        // Two NaNs are also UNKNOWN under predicate comparison.
        assert!(try_compare_values_predicate(&nan, &nan).unwrap().is_none());
        // Float32 (CQL `float`) NaN behaves identically.
        assert!(
            try_compare_values_predicate(&Value::Float32(f32::NAN), &Value::Float32(1.5))
                .unwrap()
                .is_none()
        );

        // Eq already dropped NaN (no NaN-aware equality) — confirm unchanged.
        assert!(!values_equal(&nan, &bound), "NaN = 1.5 is false");
        assert!(!values_equal(&nan, &nan), "NaN = NaN is false (SQL)");

        // Non-NaN floats keep normal ordering under predicate comparison.
        let ok = try_compare_values_predicate(&Value::Float(2.0), &bound).unwrap();
        assert!(ok.is_some_and(|o| o.is_gt()), "2.0 > 1.5 holds");
    }

    /// Issue #2231: the NaN-drop is scoped to PREDICATE comparison only — the
    /// total-order comparator (`compare_values_ordering`, used by ORDER BY /
    /// MIN / MAX / clustering) must STILL sort NaN as the greatest value.
    #[test]
    fn nan_ordering_total_order_unchanged_by_predicate_fix() {
        use std::cmp::Ordering;
        assert_eq!(
            compare_values_ordering(&Value::Float(f64::NAN), &Value::Float(1.5)),
            Ordering::Greater,
            "sort order still puts NaN last (unchanged)"
        );
        // The predicate variant agrees for non-NaN but diverges (None) on NaN.
        assert_eq!(
            compare_values_ordering_predicate(&Value::Float(2.0), &Value::Float(1.5)),
            Some(Ordering::Greater)
        );
        assert!(
            compare_values_ordering_predicate(&Value::Float(f64::NAN), &Value::Float(1.5))
                .is_none()
        );
    }

    /// `eval_scalar_comparison` dispatches each of the 6 scalar operators to the
    /// right predicate-semantics comparator (issue #2231's `mod.rs` expression
    /// path calls this directly, so its per-operator wiring deserves its own
    /// direct test rather than only transitive coverage).
    #[test]
    fn eval_scalar_comparison_dispatches_all_six_operators() {
        use ComparisonOperator::*;
        let five = Value::Integer(5);
        let three = Value::Integer(3);

        assert!(!eval_scalar_comparison(&Equal, &five, &three).unwrap());
        assert!(eval_scalar_comparison(&Equal, &five, &five).unwrap());
        assert!(eval_scalar_comparison(&NotEqual, &five, &three).unwrap());
        assert!(!eval_scalar_comparison(&NotEqual, &five, &five).unwrap());
        assert!(eval_scalar_comparison(&GreaterThan, &five, &three).unwrap());
        assert!(!eval_scalar_comparison(&GreaterThan, &three, &five).unwrap());
        assert!(eval_scalar_comparison(&GreaterThanOrEqual, &five, &five).unwrap());
        assert!(!eval_scalar_comparison(&GreaterThanOrEqual, &three, &five).unwrap());
        assert!(eval_scalar_comparison(&LessThan, &three, &five).unwrap());
        assert!(!eval_scalar_comparison(&LessThan, &five, &three).unwrap());
        assert!(eval_scalar_comparison(&LessThanOrEqual, &five, &five).unwrap());
        assert!(!eval_scalar_comparison(&LessThanOrEqual, &five, &three).unwrap());

        // A non-scalar operator is rejected rather than silently mishandled.
        assert!(eval_scalar_comparison(&In, &five, &three).is_err());
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
        let mut v = [
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

    /// Issue #3805: the empty-buffer sentinel is COMPARABLE against a
    /// non-empty value of the SAME declared type, and sorts strictly first
    /// (`db/marshal/Int32Type.java:61-71` at `cassandra-5.0.8`). Its
    /// discriminant differs, so without the dedicated branch this pair raised
    /// "Cannot compare incompatible types" and the key would have been dropped
    /// from an ORDER BY / WHERE evaluation.
    #[test]
    fn the_empty_buffer_sentinel_is_comparable_within_its_declared_type() {
        use crate::types::EmptyValueType;
        use std::cmp::Ordering;

        let empty = Value::Empty(EmptyValueType::Int);
        assert_eq!(
            try_compare_values(&empty, &Value::Integer(i32::MIN)).unwrap(),
            Ordering::Less
        );
        assert_eq!(
            try_compare_values(&Value::Integer(i32::MIN), &empty).unwrap(),
            Ordering::Greater
        );
        assert_eq!(
            try_compare_values(&empty, &Value::Empty(EmptyValueType::Int)).unwrap(),
            Ordering::Equal
        );
    }

    /// REGRESSION, roborev job 438 F2. CQLite stores BOTH CQL `uuid` and CQL
    /// `timeuuid` as `Value::Uuid([u8; 16])`, so `data_type()` answers
    /// `CqlType::Uuid` for a timeuuid too — and a naive declared-type equality
    /// check therefore REFUSED `Empty(TimeUuid)` against every non-empty
    /// timeuuid. An ordering that refuses a legitimate pair breaks the
    /// "empty sorts strictly before every non-empty value of its type"
    /// property. Both directions plus antisymmetry, for BOTH spellings of the
    /// 16-byte pair.
    #[test]
    fn the_uuid_timeuuid_pair_compares_despite_a_lossy_data_type() {
        use crate::types::EmptyValueType;
        use std::cmp::Ordering;

        // A real v1 (time) UUID and an ordinary one; `Value` cannot tell them
        // apart, which is the point.
        let non_empty = [
            Value::Uuid([0u8; 16]),
            Value::Uuid([0xff; 16]),
            Value::Uuid([
                0x58, 0xe0, 0xa7, 0xd7, 0xee, 0xbc, 0x11, 0xd8, 0x9f, 0x32, 0xf2, 0x80, 0x1f, 0x1b,
                0x9f, 0xd1,
            ]),
        ];
        for tag in [EmptyValueType::TimeUuid, EmptyValueType::Uuid] {
            let empty = Value::Empty(tag);
            for other in &non_empty {
                let fwd = try_compare_values(&empty, other)
                    .unwrap_or_else(|e| panic!("Empty({tag:?}) vs {other:?} refused: {e}"));
                let rev = try_compare_values(other, &empty)
                    .unwrap_or_else(|e| panic!("{other:?} vs Empty({tag:?}) refused: {e}"));
                assert_eq!(fwd, Ordering::Less, "Empty({tag:?}) did not sort first");
                // Antisymmetry: reversing the operands reverses the answer.
                assert_eq!(rev, Ordering::Greater, "asymmetry broken for {tag:?}");
                assert_eq!(fwd.reverse(), rev);
            }
        }
    }

    /// …and a sentinel of a DIFFERENT declared type stays INCOMPARABLE, exactly
    /// as two mismatched scalars do. Comparability is decided by the declared
    /// type, never by byte shape (no-heuristics, issue #28).
    #[test]
    fn a_sentinel_of_another_type_stays_incomparable() {
        use crate::types::EmptyValueType;

        assert!(try_compare_values(&Value::Empty(EmptyValueType::Int), &Value::BigInt(1)).is_err());
        // The 16-byte pair admission above must NOT leak into any other
        // family: a uuid sentinel is still incomparable with a non-uuid.
        assert!(
            try_compare_values(&Value::Empty(EmptyValueType::Uuid), &Value::Integer(1)).is_err()
        );
        assert!(
            try_compare_values(&Value::Empty(EmptyValueType::TimeUuid), &Value::BigInt(1)).is_err()
        );
        assert!(
            try_compare_values(&Value::Empty(EmptyValueType::Timestamp), &Value::BigInt(1))
                .is_err(),
            "timestamp and bigint are distinct CqlTypes and must stay incomparable"
        );
        assert!(try_compare_values(
            &Value::Empty(EmptyValueType::Int),
            &Value::text("x".to_string())
        )
        .is_err());
    }

    /// All 12 `EmptyValueType` tags, so a new admitted family joins the
    /// sentinel-vs-sentinel cases below automatically rather than silently
    /// staying uncovered. Kept local to these tests: `EmptyValueType` exposes
    /// no `ALL`, and inventing one would be a public-surface change nobody
    /// ruled.
    const ALL_EMPTY_TAGS: [crate::types::EmptyValueType; 12] = {
        use crate::types::EmptyValueType as E;
        [
            E::Int,
            E::BigInt,
            E::Counter,
            E::Float,
            E::Double,
            E::Timestamp,
            E::Uuid,
            E::TimeUuid,
            E::Boolean,
            E::Inet,
            E::Decimal,
            E::Varint,
        ]
    };

    /// REGRESSION, roborev job 451. Every `Value::Empty` shares ONE
    /// discriminant whatever its tag, so before the dedicated
    /// sentinel-vs-sentinel arm the discriminant test routed
    /// `Empty(Int)` vs `Empty(BigInt)` into `Value::partial_cmp`, which orders
    /// two sentinels BY TAG (it must: it is a total order). That handed a
    /// CROSS-TYPE comparison an ORDERING where this function's own rule for two
    /// different declared types is an ERROR — so a `WHERE` comparison between
    /// two differently-typed sentinels answered `Less`/`Greater` instead of
    /// diagnosing the type mismatch.
    ///
    /// Both orders for every ordered pair of DISTINCT tags: an error is
    /// symmetric, so `a` vs `b` and `b` vs `a` must BOTH refuse (an
    /// error one way and an ordering the other would be the same defect
    /// wearing one direction).
    #[test]
    fn two_sentinels_of_different_declared_types_are_incomparable() {
        for x in ALL_EMPTY_TAGS {
            for y in ALL_EMPTY_TAGS {
                if x == y {
                    continue;
                }
                let (a, b) = (Value::Empty(x), Value::Empty(y));
                assert!(
                    try_compare_values(&a, &b).is_err(),
                    "Empty({x:?}) vs Empty({y:?}) must be incomparable, not ordered"
                );
                assert!(
                    try_compare_values(&b, &a).is_err(),
                    "Empty({y:?}) vs Empty({x:?}) must be incomparable, not ordered"
                );
            }
        }
    }

    /// `uuid` and `timeuuid` are DISTINCT declared types, and for two sentinels
    /// CQLite knows which is which — the tag is carried, so nothing is lossy
    /// here. The `empty_tag_matches_operand` admission of that pair exists ONLY
    /// because a NON-EMPTY timeuuid is indistinguishable from a uuid in `Value`
    /// (`Value::Uuid([u8; 16])` for both); it must NOT leak into the
    /// sentinel-vs-sentinel case, where no such loss occurs. Pinned separately
    /// from the sweep above because this is the one pair a future reader is
    /// most likely to "fix" in the wrong direction.
    #[test]
    fn the_uuid_timeuuid_admission_does_not_leak_into_sentinel_vs_sentinel() {
        use crate::types::EmptyValueType;

        assert!(try_compare_values(
            &Value::Empty(EmptyValueType::Uuid),
            &Value::Empty(EmptyValueType::TimeUuid)
        )
        .is_err());
        assert!(try_compare_values(
            &Value::Empty(EmptyValueType::TimeUuid),
            &Value::Empty(EmptyValueType::Uuid)
        )
        .is_err());
    }

    /// The OTHER half of the same line, which slice 1's lead audit (Q3)
    /// established and roborev's finding did not touch: two sentinels of the
    /// SAME declared type are `Equal`, INCLUDING a value compared with itself.
    /// `compare_values_ordering` feeds `sort_by`, so a sentinel that did not
    /// compare `Equal` to itself would break reflexivity and can panic the
    /// sort. Asserted through BOTH entry points, because the ordering wrapper
    /// swallows errors and would hide a regression in the `Result` one.
    #[test]
    fn matching_sentinel_tags_are_equal_and_reflexive() {
        use std::cmp::Ordering;

        for tag in ALL_EMPTY_TAGS {
            let a = Value::Empty(tag);
            let b = Value::Empty(tag);
            assert_eq!(
                try_compare_values(&a, &b)
                    .unwrap_or_else(|e| panic!("Empty({tag:?}) vs Empty({tag:?}) refused: {e}")),
                Ordering::Equal
            );
            // Reflexivity proper: the SAME value, both entry points.
            assert_eq!(
                try_compare_values(&a, &a)
                    .unwrap_or_else(|e| panic!("Empty({tag:?}) vs itself refused: {e}")),
                Ordering::Equal
            );
            assert_eq!(compare_values_ordering(&a, &a), Ordering::Equal);
        }
    }

    /// The sentinel-vs-NON-EMPTY arms BELOW the discriminant test must not be
    /// weakened by the new arm above it: an empty buffer still sorts strictly
    /// before every non-empty value of its own declared type
    /// (`db/marshal/Int32Type.java:61-71` at `cassandra-5.0.8`), and the
    /// uuid/timeuuid 16-byte admission still holds. Antisymmetry on every pair.
    #[test]
    fn sentinel_versus_non_empty_ordering_survives_the_new_arm() {
        use crate::types::EmptyValueType;
        use std::cmp::Ordering;

        let pairs = [
            (EmptyValueType::Int, Value::Integer(5)),
            (EmptyValueType::Int, Value::Integer(i32::MIN)),
            (EmptyValueType::BigInt, Value::BigInt(-1)),
            (EmptyValueType::Uuid, Value::Uuid([0u8; 16])),
            (EmptyValueType::TimeUuid, Value::Uuid([0xff; 16])),
        ];
        for (tag, other) in &pairs {
            let empty = Value::Empty(*tag);
            let fwd = try_compare_values(&empty, other)
                .unwrap_or_else(|e| panic!("Empty({tag:?}) vs {other:?} refused: {e}"));
            let rev = try_compare_values(other, &empty)
                .unwrap_or_else(|e| panic!("{other:?} vs Empty({tag:?}) refused: {e}"));
            assert_eq!(fwd, Ordering::Less, "Empty({tag:?}) did not sort first");
            assert_eq!(rev, Ordering::Greater, "asymmetry broken for {tag:?}");
            assert_eq!(fwd.reverse(), rev);
        }
    }
}
