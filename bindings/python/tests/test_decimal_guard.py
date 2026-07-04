"""Corrupt-DECIMAL rendering guard (issue #1741).

The Python binding renders a CQL DECIMAL by calling ``str()`` on the unscaled
integer, which raises an *uncatchable* ``ValueError`` once the digit count
exceeds ``sys.get_int_max_str_digits()`` (py3.11+, default 4300). To keep the
interpreter abort-safe the binding refuses to stringify an unbounded/corrupt
unscaled magnitude and surfaces a typed ``CqliteError`` instead.

The guard must reject ONLY a genuinely unbounded/corrupt value, not a
large-but-representable one. An ``N``-byte SIGNED two's-complement integer has
one sign bit, so its MAGNITUDE is at most ``2^(8N-1)`` and has at most
``ceil((8N-1) * log10(2))`` decimal digits. That product is never integral, so
``ceil`` equals ``floor + 1`` — the EXACT digit-count upper bound, needing no
rounding margin. Rejecting only when this bound exceeds the interpreter limit
lets a value sitting exactly at the cap render while a truly unbounded/corrupt
byte length is still refused (fail-closed). The previous
``ceil(N * log10(256)) + 1`` added a spurious ``+1`` and over-rejected a minimal
value at the boundary (e.g. a 1785-byte integer fits in 4300 digits but the old
formula computed 4301).

These drive :func:`cqlite._decimal_from_parts` — the internal test helper that
runs the exact production conversion path (``value::decimal_to_pydecimal``) — so
no multi-kilobyte on-disk fixture is required.
"""

from __future__ import annotations

import math
import sys
from decimal import Decimal

import pytest

import cqlite

# log10(2): the guard bounds |unscaled| (magnitude 2^(8N-1)) digit count by
# ceil((8N-1) * log10(2)).
_LOG10_2 = 0.301_029_995_663_981_2


def _positive_unscaled(num_bytes: int) -> bytes:
    """A positive big-endian two's-complement magnitude of ``num_bytes`` bytes."""
    return b"\x7f" + b"\xff" * (num_bytes - 1)


def _max_digits(num_bytes: int) -> int:
    """The guard's tight digit-count upper bound for an ``num_bytes``-byte value."""
    return math.ceil((8 * num_bytes - 1) * _LOG10_2)


class TestDecimalRenderingGuard:
    def test_large_but_representable_decimal_renders(self):
        """A ~1500-byte unscaled value (~3613 digits, under the 4300 default)
        must render — the tight bound does not over-reject it."""
        num_bytes = 1500
        # Sanity: the tight bound stays under a default interpreter limit.
        assert _max_digits(num_bytes) < 4300

        value = cqlite._decimal_from_parts(2, _positive_unscaled(num_bytes))
        assert isinstance(value, Decimal)
        # Non-zero and scaled by 10^-2 (scale == 2); exact value is irrelevant.
        assert value != 0

    def test_oversized_decimal_raises_typed_error(self):
        """A value whose TIGHT digit bound exceeds the interpreter limit must
        surface a typed ``CqliteError`` (never abort), preserving the fail-closed
        guard against a corrupt/unbounded unscaled magnitude."""
        get_limit = getattr(sys, "get_int_max_str_digits", None)
        limit = get_limit() if callable(get_limit) else 0
        if limit == 0:
            pytest.skip(
                "interpreter exposes no int->str digit limit; guard uses the hard "
                "cap (1_000_000) only, not exercisable with a small byte buffer"
            )

        # Pick a byte length whose tight bound exceeds `limit`.
        num_bytes = int(limit / (8 * _LOG10_2)) + 64
        assert _max_digits(num_bytes) > limit

        with pytest.raises(cqlite.CqliteError):
            cqlite._decimal_from_parts(1, _positive_unscaled(num_bytes))

    def test_boundary_at_configured_digit_cap(self):
        """At the configured cap the guard is EXACT: the largest byte length whose
        magnitude bound is ``<= cap`` renders, and the next byte length (bound
        ``> cap``) raises. This pins that a minimal value sitting right at the cap
        is no longer over-rejected (the dropped spurious ``+1``)."""
        get_limit = getattr(sys, "get_int_max_str_digits", None)
        limit = get_limit() if callable(get_limit) else 0
        if limit == 0:
            pytest.skip("interpreter exposes no int->str digit limit; no cap boundary")

        # Smallest byte length whose bound exceeds the cap, and the one below it.
        n_over = next(n for n in range(1, limit) if _max_digits(n) > limit)
        n_under = n_over - 1
        # The boundary is genuine: under is <= cap, over is > cap.
        assert _max_digits(n_under) <= limit < _max_digits(n_over)

        # At/just under the cap: renders (its true digit count is <= the bound).
        rendered = cqlite._decimal_from_parts(2, _positive_unscaled(n_under))
        assert isinstance(rendered, Decimal)
        assert rendered != 0

        # Just over the cap: fail-closed typed error, never an interpreter abort.
        with pytest.raises(cqlite.CqliteError):
            cqlite._decimal_from_parts(2, _positive_unscaled(n_over))

    def test_scale_zero_never_reaches_guard(self):
        """scale == 0 short-circuits before the guard (no stringification of the
        unscaled int), so even a large magnitude renders as an integer Decimal."""
        value = cqlite._decimal_from_parts(0, _positive_unscaled(1500))
        assert isinstance(value, Decimal)
