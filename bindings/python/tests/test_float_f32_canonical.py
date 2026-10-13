"""A declared CQL ``float`` canonicalizes to its f32 value on every leg (issue #4350).

The CLI emits a ``float`` as the shortest f32 round-trip (``1.84``; #3777, matching
``cassandra-5.0.8:src/java/org/apache/cassandra/db/marshal/FloatType.java:115-124``,
i.e. ``Float.toString``), while both bindings hand over the exact f64 widening of
the same f32 (``1.840000033378601``). As f64 those are different numbers, so the
cross-binding comparison rounds a DECLARED ``float`` to f32 on every leg before its
exact compare. This module pins that the rounding is exact and type-driven:

* CLI ``1.84`` == binding ``1.840000033378601`` for ``float``;
* two values that differ as f32 (1.84f and the next f32 up) still differ;
* for a ``double``, ``1.84`` and ``1.840000033378601`` still differ;
* a finite value beyond FLT_MAX is refused, never rounded to an infinity.

Only the CLI's TEXT leg is rounded; a binding leg (Python, Node) must already hold
an exact f32 and is refused otherwise. Non-finite values canonicalize to the strings
``"NaN"``/``"Infinity"``/``"-Infinity"``. Each property is pinned on BOTH
canonicalizers (``canonical.py`` here directly, ``canonical.mjs`` through ``node``).
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys

import pytest

from conftest import PROJECT_ROOT

PARITY_DIR = PROJECT_ROOT / "bindings" / "parity"
if str(PARITY_DIR) not in sys.path:
    sys.path.insert(0, str(PARITY_DIR))

from canonical import (  # noqa: E402
    CanonicalError,
    canon_cli,
    canon_python,
    load_cli_json,
    parse_type,
)
from vectors import load_all  # noqa: E402

FLOAT = parse_type("float")
DOUBLE = parse_type("double")

F32_184_WIDENED = 1.840000033378601  # what both bindings return for 1.84f
F32_NEXT_UP_WIDENED = 1.8400001525878906  # 1.84f with its bits + 1
FLT_MAX = 3.4028234663852886e38


# ---------------------------------------------------------------------------
# canonical.py
# ---------------------------------------------------------------------------


def test_cli_shortest_f32_spelling_equals_the_binding_widening():
    assert canon_cli(load_cli_json("1.84"), FLOAT) == canon_python(F32_184_WIDENED, FLOAT)
    assert canon_cli(load_cli_json("1.84"), FLOAT) == F32_184_WIDENED


def test_values_that_differ_as_f32_still_differ():
    """NEGATIVE: f32 rounding is exact, not a tolerance."""
    a = canon_cli(load_cli_json("1.84"), FLOAT)
    b = canon_cli(load_cli_json("1.8400002"), FLOAT)
    assert b == F32_NEXT_UP_WIDENED
    assert a != b
    assert canon_python(F32_NEXT_UP_WIDENED, FLOAT) != canon_python(F32_184_WIDENED, FLOAT)


def test_double_is_never_rounded_to_f32():
    """NEGATIVE: a declared double stays strictly f64."""
    assert canon_cli(load_cli_json("1.84"), DOUBLE) == 1.84
    assert canon_cli(load_cli_json("1.84"), DOUBLE) != canon_python(F32_184_WIDENED, DOUBLE)


def test_float_outside_f32_range_is_refused_not_made_infinite():
    with pytest.raises(CanonicalError, match="outside the f32 range"):
        canon_python(1e39, FLOAT)
    with pytest.raises(CanonicalError, match="outside the f32 range"):
        canon_cli(load_cli_json("1e39"), FLOAT)
    # ...while the same value is an ordinary double.
    assert canon_python(1e39, DOUBLE) == 1e39


def test_flt_max_spelling_rounds_to_flt_max_not_infinity():
    # `Float.toString(Float.MAX_VALUE)` is `3.4028235E38`: above FLT_MAX as an
    # f64, but within half an f32 ulp, so it rounds TO FLT_MAX.
    assert canon_cli(load_cli_json("3.4028235e38"), FLOAT) == FLT_MAX


def test_binding_leg_must_already_hold_an_exact_f32():
    """A binding is CHECKED, never rounded: a non-f32 value is refused, so a
    binding that widened wrongly cannot be masked (#4350 hardening)."""
    with pytest.raises(CanonicalError, match="exact f32"):
        canon_python(1.84, FLOAT)
    assert canon_python(F32_184_WIDENED, FLOAT) == F32_184_WIDENED
    # A double binding value is never checked against f32.
    assert canon_python(1.84, DOUBLE) == 1.84


@pytest.mark.parametrize(
    "value, canonical",
    [(float("nan"), "NaN"), (float("inf"), "Infinity"), (float("-inf"), "-Infinity")],
)
def test_non_finite_float_canonicalizes_to_a_string(value, canonical):
    assert canon_python(value, FLOAT) == canonical
    assert canon_python(value, DOUBLE) == canonical


def test_float_negative_zero_survives_the_rounding():
    assert canon_python(-0.0, FLOAT) == "-0.0"
    assert canon_cli(load_cli_json("-0.0"), FLOAT) == "-0.0"


def test_vector_table_pins_the_distinct_pairs():
    """The vectors carry each pair; their canonicals must differ (both runners
    reproduce every vector's canonical, so this pins canonical.mjs too)."""
    by_name = {v["name"]: v for v in load_all()["vectors"]}
    assert (
        by_name["float_cli_shortest_f32_spelling"]["canonical"]
        != by_name["float_next_f32_up_stays_distinct"]["canonical"]
    )
    assert (
        by_name["double_is_never_rounded_to_f32"]["canonical"]
        != by_name["double_f32_widening_stays_f64"]["canonical"]
    )


# ---------------------------------------------------------------------------
# canonical.mjs (the Node twin)
# ---------------------------------------------------------------------------

_JS = r"""
import { canonNode, parseType, CanonicalError } from './canonical.mjs';
const F = parseType('float'), D = parseType('double');
const refusal = (fn) => {
  try { fn(); return null; } catch (e) { return e instanceof CanonicalError ? e.message : 'other:' + e; }
};
console.log(JSON.stringify({
  widened: canonNode(1.840000033378601, F),
  next_up: canonNode(1.8400001525878906, F),
  non_f32_refused: refusal(() => canonNode(1.84, F)),
  double_184: canonNode(1.84, D),
  double_widened: canonNode(1.840000033378601, D),
  flt_max: canonNode(3.4028234663852886e38, F),
  neg_zero: canonNode(-0, F),
  nan: canonNode(NaN, F),
  pos_inf: canonNode(Infinity, F),
  neg_inf: canonNode(-Infinity, F),
  out_of_range_refused: refusal(() => canonNode(1e39, F)),
}));
"""


@pytest.mark.slow
def test_node_canonicalizer_rounds_float_to_f32_exactly():
    if shutil.which("node") is None:
        pytest.skip("`node` is not on PATH")
    proc = subprocess.run(
        ["node", "--input-type=module", "-e", _JS],
        cwd=PARITY_DIR,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert proc.returncode == 0, proc.stderr
    got = json.loads(proc.stdout)
    # The Node leg is a BINDING leg: it is checked, never rounded.
    assert got["widened"] == F32_184_WIDENED
    assert got["next_up"] == F32_NEXT_UP_WIDENED != got["widened"]
    assert got["non_f32_refused"] is not None and "exact f32" in got["non_f32_refused"]
    assert got["double_184"] == 1.84 != got["double_widened"]
    assert got["flt_max"] == FLT_MAX
    assert got["neg_zero"] == "-0.0"
    assert (got["nan"], got["pos_inf"], got["neg_inf"]) == ("NaN", "Infinity", "-Infinity")
    assert got["out_of_range_refused"] is not None
    assert "outside the f32 range" in got["out_of_range_refused"]
