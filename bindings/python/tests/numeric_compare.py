"""Numeric equality for the parity harnesses — ONE implementation (issue #3505).

`values_equal` is implemented twice — `test_cli_parity` (binding vs CLI JSON)
and `test_parity` (binding vs sstabledump JSONL golden) — and both had their own
copy of "compare an int against a float". Both copies coerced through `float()`,
which rounds the EXACT side down to the LOSSY side's precision, so an exact
integer and a rounded float compared **equal**.

That mask sat at exactly the layer whose job is to catch the loss: #3505's
`json_to_py` defect (a JSON integer above `i64::MAX` reaching Python as
`1.8446744073709552e19`) was invisible because the CLI's exact
`18446744073709551615` was rounded to the same float before the comparison.

Two copies of a rule drift, and fixing one instance leaves the class open, so the
rule lives here once and both harnesses import it.
"""

from typing import Any

# The EXACT boundary, not a tolerance.
#
# Every integer with absolute value <= 2**53 is exactly representable in an
# IEEE-754 double (the mantissa is 53 bits, and 2**53 itself is representable
# because its trailing bit is zero), so below the bound `float(int_val)`
# provably cannot round: the tolerant compare that genuine FLOAT/DOUBLE columns
# need is safe there and must keep working.
#
# At or above the bound the coercion CAN round -- 2**53 + 1 collapses to 2**53 --
# so the comparison switches to exact. Python's `int == float` is mathematically
# exact (it coerces NEITHER operand; it compares the real values), so a rounded
# float correctly fails to equal the integer it was rounded from.
EXACT_FLOAT_INT_BOUND = 2**53

# Tolerances for a genuine float/float pair. Unchanged by #3505 -- float columns
# legitimately differ in their last bits between two renderers.
DEFAULT_REL_TOL = 1e-6
DEFAULT_ABS_TOL = 1e-9


def is_number(value: Any) -> bool:
    """An `int` or `float` that is NOT a `bool`.

    `bool` subclasses `int` in Python, so `isinstance(True, int)` is `True` and
    without this exclusion `True` and `1.0` compare equal through the numeric
    path. A CQL `boolean` renders as JSON `true`/`false`, never as a number, so
    a bool paired with a number is a genuine type mismatch a parity harness must
    report rather than coerce (issue #3505).
    """
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def is_bool_number_mismatch(a: Any, b: Any) -> bool:
    """True when exactly one side is a `bool` and the other is a number.

    Both-bool is not a mismatch; bool-vs-non-numeric is somebody else's branch.
    """
    if isinstance(a, bool) == isinstance(b, bool):
        return False
    return isinstance(a, (bool, int, float)) and isinstance(b, (bool, int, float))


def float_equal(
    a: float,
    b: float,
    rel_tol: float = DEFAULT_REL_TOL,
    abs_tol: float = DEFAULT_ABS_TOL,
) -> bool:
    """Compare two floats with tolerance, treating NaN == NaN as equal."""
    if a == b:
        return True
    if a != a and b != b:  # both NaN
        return True
    if a != a or b != b:  # exactly one NaN -- never equal
        # Explicit rather than relying on NaN propagating through the arithmetic
        # below (it does, but implicitly). This branch is verbatim from the
        # `test_cli_parity._float_equal` this module replaced.
        return False
    return abs(a - b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol)


def numbers_equal(a: Any, b: Any) -> bool:
    """Compare a numeric pair without masking precision loss (issue #3505).

    Callers must have established `is_number(a) and is_number(b)`.

    - two ints -> exact (no float ever involved)
    - int/float pair, |int| <= EXACT_FLOAT_INT_BOUND -> tolerant, as before
    - int/float pair, |int| >  EXACT_FLOAT_INT_BOUND -> EXACT
    - two floats -> tolerant
    """
    a_is_int = isinstance(a, int)
    b_is_int = isinstance(b, int)
    if a_is_int and b_is_int:
        return a == b
    if a_is_int or b_is_int:
        int_val = a if a_is_int else b
        if abs(int_val) > EXACT_FLOAT_INT_BOUND:
            # `==` between an int and a float is exact in Python; do NOT coerce.
            return a == b
    return float_equal(float(a), float(b))
