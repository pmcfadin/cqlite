"""Corrupt-DECIMAL rendering policy (issues #1741, #1754, #1452).

Since issue #1452 there is exactly ONE DECIMAL implementation and ONE rendering
policy, in ``cqlite-ffi-common``, shared by both language bindings:

* a magnitude beyond ``DECIMAL_MAX_UNSCALED_BYTES`` (32 KiB) is refused as
  corrupt with a typed error;
* below that ceiling the render is **infallible** — a well-formed
  arbitrary-precision value always renders, in precision-preserving exponent form
  when a positional expansion would be huge.

**What changed for Python** (recorded in ``CHANGELOG.md``): the previous guard was
keyed on ``sys.get_int_max_str_digits()`` (default 4300) because the old body
called Python ``str()`` on the unscaled *Python int*, which raises an uncatchable
``ValueError`` past that limit. Rust now renders the digits, so that failure mode
is structurally gone and the interpreter limit is irrelevant. A well-formed
2000-byte magnitude — which used to raise here while the Node binding rendered
it — now renders in both bindings.

What is PRESERVED is the guarantee the guard existed for (issues #1437/#1440): a
corrupt SSTable raises a typed, catchable ``CqliteError``; it never aborts the
interpreter.

These drive :func:`cqlite._decimal_from_parts` — the internal test helper that
runs the exact production conversion path (``value::decimal_to_pydecimal``) — so
no multi-kilobyte on-disk fixture is required.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

import cqlite

# The single documented refusal ceiling: `cqlite_ffi_common::decimal::
# DECIMAL_MAX_UNSCALED_BYTES`. Stated here so a change to the shared policy makes
# this suite fail rather than silently drift.
DECIMAL_MAX_UNSCALED_BYTES = 32 * 1024

# CPython's documented default `int` -> `str` conversion limit (py3.11+), which is
# what the removed #1741 guard was keyed on. A literal, because the live
# `sys.get_int_max_str_digits()` is process-global mutable state.
CPYTHON_DEFAULT_INT_STR_DIGITS = 4300


def _positive_unscaled(num_bytes: int) -> bytes:
    """A positive big-endian two's-complement magnitude of ``num_bytes`` bytes."""
    return b"\x7f" + b"\xff" * (num_bytes - 1)


class TestDecimalRenderingPolicy:
    def test_large_but_representable_decimal_renders(self):
        """A ~1500-byte unscaled value (~3613 digits) renders."""
        value = cqlite._decimal_from_parts(2, _positive_unscaled(1500))
        assert isinstance(value, Decimal)
        assert value != 0

    def test_magnitude_over_the_interpreter_digit_limit_now_renders(self):
        """The behaviour change of issue #1452, asserted directly.

        A 2000-byte magnitude has 4817 digits — above CPython's default 4300
        ``int``->``str`` limit, which is exactly what the old guard refused. Rust
        renders the digits now, so it must render, with every digit preserved.
        """
        value = cqlite._decimal_from_parts(3, _positive_unscaled(2000))
        assert isinstance(value, Decimal)
        # `as_tuple` is exact and context-free — and needs no int->str conversion,
        # which is the very thing the interpreter limit would refuse.
        sign, digits, exponent = value.as_tuple()
        assert sign == 0
        assert exponent == -3
        assert len(digits) == 4817
        # The premise of the test: 4817 digits really is past the limit the old
        # guard keyed on. Compared against CPython's DOCUMENTED DEFAULT rather
        # than the live `sys.get_int_max_str_digits()`, which is process-global
        # MUTABLE state that another test in the same session can raise.
        assert len(digits) > CPYTHON_DEFAULT_INT_STR_DIGITS

    def test_pathological_scale_renders_instead_of_raising(self):
        """``scale`` is only an exponent, so no scale value makes a well-formed
        magnitude un-renderable — including ``i32::MIN``, where the old code
        would have overflowed negating it."""
        assert cqlite._decimal_from_parts(2**31 - 1, b"\x01") == Decimal(
            "1e-2147483647"
        )
        assert cqlite._decimal_from_parts(-(2**31), b"\x01") == Decimal(
            "1e2147483648"
        )

    def test_magnitude_just_under_the_ceiling_renders(self):
        """AT the documented ceiling the value is well-formed and must render."""
        value = cqlite._decimal_from_parts(
            0, _positive_unscaled(DECIMAL_MAX_UNSCALED_BYTES)
        )
        assert isinstance(value, Decimal)
        assert value != 0

    def test_magnitude_past_the_ceiling_raises_a_typed_catchable_error(self):
        """The fail-closed half of the policy, and the abort-safety guarantee.

        One byte past the ceiling must raise a typed ``CqliteError`` naming the
        scale, the unscaled length and the ceiling — never abort the interpreter,
        and never render.
        """
        oversized = _positive_unscaled(DECIMAL_MAX_UNSCALED_BYTES + 1)
        with pytest.raises(cqlite.CqliteError) as excinfo:
            cqlite._decimal_from_parts(3, oversized)
        message = str(excinfo.value)
        assert "scale=3" in message
        assert f"unscaled_len={DECIMAL_MAX_UNSCALED_BYTES + 1} bytes" in message
        assert f"max_unscaled={DECIMAL_MAX_UNSCALED_BYTES} bytes" in message

    def test_ceiling_is_exact_at_the_boundary(self):
        """The boundary is a single byte wide: at the ceiling renders, one past
        raises. Pins that the policy is not accidentally widened or narrowed."""
        assert isinstance(
            cqlite._decimal_from_parts(
                2, _positive_unscaled(DECIMAL_MAX_UNSCALED_BYTES)
            ),
            Decimal,
        )
        with pytest.raises(cqlite.CqliteError):
            cqlite._decimal_from_parts(
                2, _positive_unscaled(DECIMAL_MAX_UNSCALED_BYTES + 1)
            )

    def test_scale_zero_large_magnitude_renders_as_an_integer(self):
        """``scale == 0`` renders the bare integer, at any representable size."""
        value = cqlite._decimal_from_parts(0, _positive_unscaled(1500))
        assert isinstance(value, Decimal)
        assert value == value.to_integral_value()

    def test_empty_unscaled_is_zero(self):
        assert cqlite._decimal_from_parts(0, b"") == Decimal("0")
        assert cqlite._decimal_from_parts(7, b"") == Decimal("0")
