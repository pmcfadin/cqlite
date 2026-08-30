"""`values_equal` must not mask integer/float precision loss (issue #3505).

Why this file exists, and why it is separate from the two parity suites:

`values_equal` is implemented TWICE — `test_cli_parity.values_equal` (binding vs
CLI JSON) and `test_parity.values_equal` (binding vs sstabledump JSONL golden) —
and both coerced an `int`/`float` pair through `float()` before comparing. That
coercion rounds the *exact* side down to the *lossy* side's precision, so an
exact integer and a rounded float compared **equal**. It is a mask sitting at
exactly the layer whose job is to catch the loss: issue #3505's `json_to_py`
defect (a JSON integer above `i64::MAX` reaching Python as
`1.8446744073709552e19`) was invisible to the harness because the CLI's exact
`18446744073709551615` was rounded to the same float.

The mask is a CLASS with two instances, so the contract is asserted ONCE here,
parametrized over both implementations, rather than duplicated into each suite.

The bound is `2**53`, and it is exact rather than a tolerance: every integer with
absolute value `<= 2**53` is exactly representable in an IEEE-754 double, so
below the bound `float(int_val)` provably cannot round and the tolerant compare
that genuine `float`/`double` columns need stays in force. At or above it the
coercion can round, so the comparison becomes exact — Python's `int == float` is
mathematically exact (it does not coerce either operand), so a rounded float no
longer equals the exact integer.
"""

import sys
from pathlib import Path

import pytest

# Both `values_equal` implementations live inside the parity suites themselves;
# import them as siblings (pytest's rootdir-prepend import mode already puts this
# directory on `sys.path`, but be explicit so a direct `python3` run works too).
sys.path.insert(0, str(Path(__file__).parent))

from test_cli_parity import values_equal as cli_values_equal  # noqa: E402
from test_parity import values_equal as jsonl_values_equal  # noqa: E402

# The two implementations under one contract. `test_cli_parity`'s parameters are
# (python_value, cli_value); `test_parity`'s are (actual, expected) where
# `expected` comes from the JSONL golden. For every case below the roles are
# symmetric — each is asserted in BOTH argument orders — so one table covers both.
IMPLS = [
    pytest.param(cli_values_equal, id="test_cli_parity"),
    pytest.param(jsonl_values_equal, id="test_parity"),
]

U64_MAX = 18446744073709551615
# What `u64::MAX` becomes once it has been through an f64. Written as a literal,
# not computed, so the test states the precision being lost.
U64_MAX_AS_F64 = 1.8446744073709552e19


@pytest.mark.parametrize("values_equal", IMPLS)
def test_exact_int_vs_rounded_float_above_2_53_is_a_mismatch(values_equal):
    """The #3505 mask itself: exact `u64::MAX` vs its rounded float must FAIL."""
    assert float(U64_MAX) == U64_MAX_AS_F64, "premise: u64::MAX rounds to this float"
    assert U64_MAX != U64_MAX_AS_F64, "premise: the rounding is real, not a no-op"

    assert values_equal(U64_MAX_AS_F64, U64_MAX) is False
    assert values_equal(U64_MAX, U64_MAX_AS_F64) is False


@pytest.mark.parametrize("values_equal", IMPLS)
def test_small_int_float_pairs_still_compare_equal(values_equal):
    """Genuine float columns are untouched below the bound."""
    assert values_equal(3, 3.0) is True
    assert values_equal(3.0, 3) is True
    assert values_equal(0, 0.0) is True
    assert values_equal(-42, -42.0) is True


@pytest.mark.parametrize("values_equal", IMPLS)
def test_float_tolerance_is_preserved_for_float_pairs(values_equal):
    """The rel/abs tolerance for two floats is not what #3505 tightens."""
    assert values_equal(0.1 + 0.2, 0.3) is True


@pytest.mark.parametrize("values_equal", IMPLS)
def test_at_the_2_53_bound_the_float_is_exact_so_it_matches(values_equal):
    """`2**53` IS exactly representable, so the pair is genuinely equal."""
    bound = 2**53
    assert float(bound) == bound, "premise: exactly representable at the bound"
    assert values_equal(bound, float(bound)) is True
    assert values_equal(float(bound), bound) is True


@pytest.mark.parametrize("values_equal", IMPLS)
def test_just_above_the_bound_the_float_cannot_represent_the_int(values_equal):
    """`2**53 + 1` is the first integer an f64 cannot hold: collapses to `2**53`."""
    above = 2**53 + 1
    assert float(above) == 2**53, "premise: float(2**53+1) collapses to 2**53"
    assert above != float(above), "premise: so the pair is NOT equal"
    assert values_equal(above, float(above)) is False
    assert values_equal(float(above), above) is False


@pytest.mark.parametrize("values_equal", IMPLS)
def test_a_large_int_exactly_representable_in_f64_still_matches(values_equal):
    """The tightening is EXACTNESS, not a blanket rejection of large integers.

    `10**19` is `2**19 * 5**19` and `5**19 < 2**53`, so it survives an f64
    round trip intact and must still compare equal above the bound.
    """
    exact_big = 10**19
    assert float(exact_big) == exact_big, "premise: exactly representable"
    assert values_equal(exact_big, float(exact_big)) is True
    assert values_equal(float(exact_big), exact_big) is True


@pytest.mark.parametrize("values_equal", IMPLS)
def test_bool_is_not_a_numeric_match(values_equal):
    """`isinstance(True, int)` is `True`, so `True` vs `1.0` coerced equal."""
    assert True == 1.0, "premise: Python's own == says these are equal"
    assert values_equal(True, 1.0) is False
    assert values_equal(1.0, True) is False
    assert values_equal(False, 0.0) is False
    assert values_equal(0.0, False) is False


@pytest.mark.parametrize("values_equal", IMPLS)
def test_identical_large_integers_still_match(values_equal):
    """No int/float coercion involved: exact integers compare exactly."""
    assert values_equal(U64_MAX, U64_MAX) is True
    assert values_equal(True, True) is True
