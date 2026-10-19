"""A CQL ``decimal`` read from the CLI's JSON must never pass through a float (issue #4350).

Since #3644 the CLI JSON egress renders a ``decimal`` as an UNQUOTED JSON number,
per ``cassandra-5.0.8:src/java/org/apache/cassandra/db/marshal/DecimalType.java:314-317``
(``toJSONString`` is a bare ``BigDecimal.toString()``). Python's default
``json.loads`` turns such a number into a binary ``float``, which keeps only ~17
significant digits. A comparison made after that parse is no longer a decimal
comparison at all: ``12345678901234567890.123456789`` and
``12345678901234567890.1`` become the SAME float.

Both harnesses that read CLI JSON therefore parse it so the number's TEXT
survives (``parse_float=decimal.Decimal``), and both REFUSE a float where a
``decimal`` is declared. This module pins that, with a value carrying more
significant digits than an f64 can hold, plus the negative controls: a
float-rounded value, a binary float and a quoted string are each NOT accepted
as equal.

The live three-way pin over REAL data is the ``signed_special_decimals`` fixture
in ``bindings/parity/fixtures.json`` (33-significant-digit decimals written by
Cassandra), run by ``test_cross_binding_parity.py::test_three_way_parity``.
"""

from __future__ import annotations

import json
import sys
from decimal import Decimal

import pytest

from conftest import PROJECT_ROOT

PARITY_DIR = PROJECT_ROOT / "bindings" / "parity"
if str(PARITY_DIR) not in sys.path:
    sys.path.insert(0, str(PARITY_DIR))

from canonical import (  # noqa: E402
    CanonicalError,
    canon_cli,
    canon_python,
    canon_row_cli,
    load_cli_json,
    normalize_decimal_string,
    parse_type,
)

import test_cli_parity as cli_parity  # noqa: E402

# 29 significant digits: an f64 holds 15-17, so any float round-trip loses the
# tail. This exact literal is #4350's acceptance criterion.
WIDE = "12345678901234567890.123456789"
# What a float makes of it (repr of the nearest double).
WIDE_AS_FLOAT_TEXT = repr(float(WIDE))

DECIMAL = parse_type("decimal")


def test_the_wide_value_really_does_not_survive_a_float():
    """The premise of every other test here: f64 cannot carry WIDE."""
    assert Decimal(WIDE_AS_FLOAT_TEXT) != Decimal(WIDE)
    assert json.loads(WIDE) == json.loads("12345678901234567890.1")  # the hazard


# ---------------------------------------------------------------------------
# Cross-binding canonicalizer (bindings/parity/canonical.py)
# ---------------------------------------------------------------------------


def test_cli_json_loader_preserves_the_decimal_text():
    parsed = load_cli_json(f'[{{"d": {WIDE}}}]')
    assert isinstance(parsed[0]["d"], Decimal)
    assert str(parsed[0]["d"]) == WIDE


def test_wide_decimal_canonicalizes_identically_on_cli_and_python_legs():
    cli = canon_cli(load_cli_json(WIDE), DECIMAL)
    py = canon_python(Decimal(WIDE), DECIMAL)
    assert cli == py == WIDE


def test_wide_decimal_inside_a_set_survives_the_cli_leg():
    t = parse_type("set<decimal>")
    cli = canon_cli(load_cli_json(f"[{WIDE}, -1.5]"), t)
    py = canon_python(frozenset({Decimal(WIDE), Decimal("-1.5")}), t)
    assert cli == py
    assert WIDE in cli


def test_float_rounded_cli_value_is_not_equal_to_the_exact_decimal():
    """NEGATIVE: the f64-rounded spelling must canonicalize to something else."""
    rounded = canon_cli(load_cli_json(WIDE_AS_FLOAT_TEXT), DECIMAL)
    assert rounded != canon_python(Decimal(WIDE), DECIMAL)


def test_cli_leg_refuses_a_binary_float_for_a_declared_decimal():
    """NEGATIVE: a float means the exact text was already lost — refuse it."""
    with pytest.raises(CanonicalError, match="float"):
        canon_cli(float(WIDE), DECIMAL)
    # ...which is exactly what a DEFAULT json.loads hands the canonicalizer.
    with pytest.raises(CanonicalError, match="float"):
        canon_row_cli(json.loads(f'{{"d": {WIDE}}}'), {"d": DECIMAL})


def test_cli_leg_refuses_a_quoted_decimal():
    """NEGATIVE: #3644 made the egress unquoted; a string is a regression."""
    with pytest.raises(CanonicalError, match="JSON number"):
        canon_cli(WIDE, DECIMAL)


@pytest.mark.parametrize(
    "doc", ['{"x": NaN}', '{"x": Infinity}', '{"x": -Infinity}', "[1.5, NaN]"]
)
def test_cli_json_loader_refuses_non_json_constants(doc):
    """NaN/Infinity are not JSON; the CLI renders a non-finite value as null.
    A CLI regressing to emit the bare token must red, not pass (#4350)."""
    with pytest.raises(CanonicalError, match="non-JSON constant"):
        load_cli_json(doc)
    with pytest.raises(CanonicalError, match="non-JSON constant"):
        cli_parity.parse_cli_json(doc)
    assert load_cli_json('{"x": null}') == {"x": None}


@pytest.mark.parametrize("text", ["-0", "-0.00", "-0.0000000", "-0E-8"])
def test_cli_leg_refuses_a_negative_zero_decimal(text):
    """A BigDecimal has no negative zero, so the CLI cannot legitimately emit
    one: refused on the cli leg rather than folded (#4350)."""
    with pytest.raises(CanonicalError, match="negative-zero"):
        canon_cli(load_cli_json(text), DECIMAL)
    assert canon_cli(load_cli_json(text.lstrip("-")), DECIMAL) is not None


def test_cli_leg_keeps_the_scale_of_a_fractional_decimal():
    """Scale matters, exactly as on the python/node legs: 1.50 != 1.5."""
    assert canon_cli(load_cli_json("1.50"), DECIMAL) == "1.50"
    assert canon_cli(load_cli_json("1.50"), DECIMAL) != canon_cli(load_cli_json("1.5"), DECIMAL)


def test_cli_leg_accepts_an_integral_and_an_exponent_decimal_exactly():
    # BigDecimal.toString spells unscaled=1, scale=-3 as `1E+3`; the CLI
    # expands it positionally to `1000`. Both reduce to ONE canonical string.
    assert canon_cli(load_cli_json("1000"), DECIMAL) == canon_python(Decimal("1E+3"), DECIMAL)
    assert canon_cli(load_cli_json("1E+3"), DECIMAL) == "1e3"
    # An integral decimal wider than Python's int-from-str digit limit (4300)
    # still parses exactly, because the loader never builds an `int` from it.
    huge = "7" + "0" * 5000
    assert canon_cli(load_cli_json(huge), DECIMAL) == "7e5000"


def test_cli_loader_keeps_integer_and_float_columns_unchanged():
    """The lossless parse must not change any OTHER declared type's result."""
    assert canon_cli(load_cli_json("42"), parse_type("int")) == 42
    assert canon_cli(load_cli_json("1.67"), parse_type("double")) == 1.67
    # A declared float is its f32 value (see test_float_f32_canonical.py).
    assert canon_cli(load_cli_json("1.67"), parse_type("float")) == 1.6699999570846558
    assert canon_cli(load_cli_json("0.1"), parse_type("double")) == 0.1
    # A fractional JSON number is still NOT an integer.
    with pytest.raises(CanonicalError, match="integer"):
        canon_cli(load_cli_json("42.0"), parse_type("int"))


def test_python_decimal_parser_still_strips_leading_zeros():
    """The two leading-zero parser probes' cli legs are now CLI-unreachable
    nulls (`0000123.4500` is not a valid JSON number), so the Python string
    parser's leading-zero branch is pinned here directly."""
    assert normalize_decimal_string("0000123.4500") == "123.4500"
    assert normalize_decimal_string("0001e5000") == "1e5000"


# ---------------------------------------------------------------------------
# Python-vs-CLI parity harness (bindings/python/tests/test_cli_parity.py)
# ---------------------------------------------------------------------------


def _cli_parity_equal(py_value, cli_json_text: str) -> bool:
    cli_value = cli_parity.parse_cli_json(cli_json_text)
    return cli_parity.values_equal(
        cli_parity.normalize_python_value(py_value, is_row_level=False),
        cli_parity.normalize_cli_value(cli_value),
    )


def test_cli_parity_parse_preserves_the_decimal_text():
    assert str(cli_parity.parse_cli_json(WIDE)) == WIDE


def test_cli_parity_wide_decimal_compares_exactly():
    assert _cli_parity_equal(Decimal(WIDE), WIDE)


def test_cli_parity_rejects_a_float_rounded_decimal():
    """NEGATIVE: the f64-rounded text is a DIFFERENT decimal."""
    assert not _cli_parity_equal(Decimal(WIDE), WIDE_AS_FLOAT_TEXT)


def test_cli_parity_rejects_a_binary_float_for_a_decimal():
    """NEGATIVE: a float (what a default json.loads yields) is never equal."""
    py = cli_parity.normalize_python_value(Decimal(WIDE), is_row_level=False)
    assert not cli_parity.values_equal(py, float(WIDE))
    assert not cli_parity.values_equal(py, json.loads(WIDE))


def test_cli_parity_rejects_a_quoted_decimal():
    py = cli_parity.normalize_python_value(Decimal("69799.73"), is_row_level=False)
    assert not cli_parity.values_equal(py, "69799.73")


def test_cli_parity_decimal_scale_is_significant():
    assert _cli_parity_equal(Decimal("1.50"), "1.50")
    assert not _cli_parity_equal(Decimal("1.50"), "1.5")


def test_cli_parity_set_of_wide_decimals_compares_exactly():
    py = frozenset({Decimal(WIDE), Decimal("0"), Decimal("-1.5")})
    assert _cli_parity_equal(py, f"[-1.5, 0, {WIDE}]")
    assert not _cli_parity_equal(py, f"[-1.5, 0, {WIDE_AS_FLOAT_TEXT}]")


def test_cli_parity_shares_the_one_cli_json_loader():
    """ONE loader (#4350 review N3): an integral decimal wider than Python's
    4300-digit int-from-str limit parses and compares exactly."""
    huge = "7" + "0" * 5000
    assert cli_parity.parse_cli_json(f"[{huge}]") == [7 * 10**5000]
    assert _cli_parity_equal(Decimal("7E+5000"), huge)
    assert not _cli_parity_equal(Decimal("7E+5000"), "8" + "0" * 5000)
    # Ordinary integers are still plain ints for the integer comparisons.
    assert type(cli_parity.parse_cli_json("[42]")[0]) is int


def test_cli_parity_float_columns_still_compare_as_floats():
    """A double column's fractional JSON number is still compared as a float."""
    assert _cli_parity_equal(0.1, "0.1")
    assert _cli_parity_equal(1.6699999570846558, "1.6699999570846558")
    assert not _cli_parity_equal(0.1, "0.2")


# A finite JSON number past the f64 range. load_cli_json keeps it as an exact
# Decimal (or CliJsonInteger), and float() of either is +/-inf, NOT an error.
# The CLI renders a genuinely non-finite double as `null` (README gap 4), so
# such a token can only be a renderer defect; it must never canonicalize as
# "Infinity" (roborev job 7, finding 1).
F64_OVERFLOW_TOKENS = ["1e400", "-1e400", "1.5E+309", "1" + "0" * 400, "-" + "1" + "0" * 400]


@pytest.mark.parametrize("token", F64_OVERFLOW_TOKENS)
@pytest.mark.parametrize("kind", ["double", "float"])
def test_cli_leg_refuses_a_finite_token_past_the_f64_range(token, kind):
    with pytest.raises(CanonicalError, match="outside the f64 range"):
        canon_cli(load_cli_json(token), parse_type(kind))
    # Control: the largest finite double still canonicalizes as itself.
    assert canon_cli(load_cli_json("1.7976931348623157e308"), parse_type("double")) == 1.7976931348623157e308


@pytest.mark.parametrize("token", F64_OVERFLOW_TOKENS)
def test_cli_parity_never_equates_a_finite_overflow_token_with_infinity(token):
    infinity = float("-inf") if token.startswith("-") else float("inf")
    assert not _cli_parity_equal(infinity, token)


@pytest.mark.parametrize("text", ["-0", "-0.00", "-0E-8"])
def test_cli_parity_never_equates_a_negative_zero_decimal(text):
    """A BigDecimal has no negative zero, so a signed-zero token is a renderer
    defect and must not compare equal to the binding's zero. `-0` used to lose
    its sign at int() and `-0.00` at normalize_decimal_string (roborev job 7,
    finding 2)."""
    unsigned = text.lstrip("-")
    assert _cli_parity_equal(Decimal(unsigned), unsigned)  # control
    assert not _cli_parity_equal(Decimal(unsigned), text)
    assert not _cli_parity_equal(Decimal("0"), text)


def test_cli_parity_keeps_the_sign_of_a_negative_zero_integer_token():
    """The integer-token conversion must not erase the sign it is asked to judge."""
    (token,) = cli_parity.parse_cli_json("[-0]")
    assert isinstance(token, Decimal) and token.is_zero() and token.is_signed()
    assert type(cli_parity.parse_cli_json("[0]")[0]) is int


# The other end of the range (roborev job 8): a finite NONZERO token that
# float() turns into 0.0 (f64 underflow), or that a declared float's f32
# rounding turns into 0.0. The CLI prints the shortest round-trip of a stored
# value, so neither can be a real zero.
@pytest.mark.parametrize("token", ["1e-400", "-1e-400", "4.9e-325"])
@pytest.mark.parametrize("kind", ["double", "float"])
def test_cli_leg_refuses_a_nonzero_token_that_underflows_f64(token, kind):
    with pytest.raises(CanonicalError, match="underflows to zero in f64"):
        canon_cli(load_cli_json(token), parse_type(kind))


@pytest.mark.parametrize("token", ["1e-50", "-1e-50", "7e-46"])
def test_cli_leg_refuses_a_nonzero_float_token_that_underflows_f32(token):
    with pytest.raises(CanonicalError, match="underflows to zero in f32"):
        canon_cli(load_cli_json(token), parse_type("float"))
    # The same token is a legitimate (subnormal or normal) double.
    assert canon_cli(load_cli_json(token), parse_type("double")) == float(token)


def test_cli_leg_keeps_real_zeros_and_the_smallest_nonzero_values():
    """Controls: a real zero is still a zero, and the smallest nonzero f64/f32
    (as the CLI would print them) still canonicalize as themselves."""
    assert canon_cli(load_cli_json("0.0"), parse_type("double")) == 0.0
    assert canon_cli(load_cli_json("0"), parse_type("float")) == 0.0
    assert canon_cli(load_cli_json("0E-8"), parse_type("double")) == 0.0
    assert canon_cli(load_cli_json("5e-324"), parse_type("double")) == 5e-324
    assert canon_cli(load_cli_json("1.4E-45"), parse_type("float")) == 1.401298464324817e-45


@pytest.mark.parametrize("token", ["1e-400", "-1e-400"])
def test_cli_parity_never_equates_an_underflowing_token_with_zero(token):
    assert _cli_parity_equal(0.0, "0.0")  # control
    assert not _cli_parity_equal(0.0, token)
    assert not _cli_parity_equal(-0.0, token)
