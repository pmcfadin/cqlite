"""Cross-binding shared-vector assertions (issue #1452).

``cqlite-ffi-common`` holds exactly ONE implementation of CQL ``decimal``,
``varint`` and ``inet`` rendering, and exports a committed table of
``(input, expected outcome)`` vectors. This suite renders **every** entry through
this binding's production conversion path and asserts the outcome; the Node
binding's ``__test__/shared-vectors.test.js`` asserts the *same committed table*
through *its* production path.

That is what makes "single implementation" an assertion rather than a comment: a
divergence between the two bindings — or a re-introduced private copy in either —
fails BOTH suites. Adding an entry to the shared crate covers it in both suites
with no per-binding edit.

The two comparison rules come from the shared crate's ``VectorOutcome`` contract:

* ``kind == "value"`` → the path must render, and ``actual`` must equal
  ``expected`` exactly. A multi-thousand-digit rendering is compared as a
  *digest* (a digit run longer than 64 collapses to ``{<length>}``), which still
  pins the exact digit count and the exact surrounding form.
* ``kind == "error"`` → the path must refuse, and ``expected`` must appear
  **verbatim inside** ``actual``. Containment only because each binding wraps the
  one canonical message in its own typed-error envelope.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

import cqlite


def _vectors():
    """The committed table, rendered through this binding's production paths."""
    return cqlite._ffi_common_render_vectors()


def _by_type(cql_type: str):
    return [entry for entry in _vectors() if entry["cql_type"] == cql_type]


def test_vector_table_is_present_and_covers_all_three_types():
    """A vacuous pass is impossible: the table must be non-empty per type.

    Without this, an empty table would make every parametrised assertion below
    trivially satisfied.
    """
    entries = _vectors()
    assert entries, "the shared vector table must not be empty"
    for cql_type in ("decimal", "varint", "inet"):
        assert _by_type(cql_type), f"no {cql_type} vectors were reported"
    # Each entry name is unique, so a failure names one input unambiguously.
    names = [entry["name"] for entry in entries]
    assert len(set(names)) == len(names)
    # The refusal path is covered, not just the happy path.
    assert any(entry["kind"] == "error" for entry in entries)


@pytest.mark.parametrize("entry", _vectors(), ids=lambda e: e["name"])
def test_every_vector_renders_as_the_committed_table_says(entry):
    """One expectation per entry, for the WHOLE table."""
    if entry["kind"] == "value":
        assert entry["outcome"] == "ok", (
            f"{entry['name']}: expected a rendering, the production path refused "
            f"with: {entry['actual']}"
        )
        assert entry["actual"] == entry["expected"], entry["name"]
    elif entry["kind"] == "error":
        assert entry["outcome"] == "err", (
            f"{entry['name']}: expected a refusal, the production path rendered "
            f"{entry['actual']!r}"
        )
        assert entry["expected"] in entry["actual"], entry["name"]
    else:  # pragma: no cover - fail closed on an unknown kind
        pytest.fail(f"{entry['name']}: unknown expectation kind {entry['kind']!r}")


@pytest.mark.parametrize(
    "entry",
    [e for e in _by_type("decimal") if e["kind"] == "value" and "{" not in e["expected"]],
    ids=lambda e: e["name"],
)
def test_decimal_full_object_path_yields_the_expected_value(entry):
    """The FULL production object path, not just the rendered text.

    ``_ffi_common_render_vectors`` reports the rendered *text* for DECIMAL
    because ``Decimal.__str__`` re-normalises exponent form. This closes the
    remaining step by driving ``_decimal_from_parts`` — i.e.
    ``value::decimal_to_pydecimal`` — and comparing ``Decimal`` VALUES, which are
    immune to that re-formatting. Digest entries are excluded because a digest is
    not a parseable number.
    """
    rendered = cqlite._decimal_from_parts(entry["scale"], entry["bytes"])
    assert isinstance(rendered, Decimal)
    assert rendered == Decimal(entry["expected"]), entry["name"]


@pytest.mark.parametrize(
    "entry",
    [e for e in _by_type("decimal") if e["kind"] == "error"],
    ids=lambda e: e["name"],
)
def test_decimal_refusal_is_a_typed_catchable_error(entry):
    """A refused DECIMAL raises ``CqliteError`` — it never aborts the interpreter.

    This is the #1741/#1437/#1440 abort-safety guarantee, preserved through the
    extraction: the refusal is typed and catchable, and its message is the one
    canonical text from the shared crate.
    """
    with pytest.raises(cqlite.CqliteError) as excinfo:
        cqlite._decimal_from_parts(entry["scale"], entry["bytes"])
    assert entry["expected"] in str(excinfo.value)


@pytest.mark.parametrize(
    "entry",
    [e for e in _by_type("varint") if e["kind"] == "value"],
    ids=lambda e: e["name"],
)
def test_varint_full_object_path_yields_the_expected_int(entry):
    """``_varint_from_bytes`` drives ``value::varint_to_pyint`` directly."""
    decoded = cqlite._varint_from_bytes(entry["bytes"])
    assert isinstance(decoded, int)
    assert decoded == int(entry["expected"]), entry["name"]


@pytest.mark.parametrize("entry", _by_type("inet"), ids=lambda e: e["name"])
def test_inet_full_object_path_matches_the_table(entry):
    """``_inet_from_bytes`` drives ``value::inet_to_py`` directly.

    A malformed length must raise the typed error carrying the ONE canonical
    message — never return raw bytes, a hex string or any other passthrough
    (no-heuristics, issue #28).
    """
    if entry["kind"] == "value":
        addr = cqlite._inet_from_bytes(entry["bytes"])
        assert str(addr) == entry["expected"], entry["name"]
        assert not isinstance(addr, (bytes, bytearray, str))
    else:
        with pytest.raises(cqlite.CqliteError) as excinfo:
            cqlite._inet_from_bytes(entry["bytes"])
        assert entry["expected"] in str(excinfo.value)


def test_the_2000_byte_decimal_that_used_to_diverge_now_renders():
    """The concrete divergence issue #1452 closed.

    Before the extraction, a 2000-byte well-formed unscaled magnitude with
    ``scale = 3`` rendered in the Node binding (exponent form) and raised
    ``CqliteError`` in Python (its digit count exceeded
    ``sys.get_int_max_str_digits()``, default 4300). One implementation, one
    policy: it now renders in both, and every digit is preserved.
    """
    rendered = cqlite._decimal_from_parts(3, bytes([0x7F] * 2000))
    assert isinstance(rendered, Decimal)
    # `as_tuple` is exact and context-free: no rounding, and no int->str
    # conversion (which is exactly what CPython's 4300-digit limit would refuse).
    sign, digits, exponent = rendered.as_tuple()
    assert sign == 0
    # 4817 significant digits (CPython `int`, cross-checked with floor(log10)+1).
    assert len(digits) == 4817
    assert exponent == -3


def test_scale_i32_min_renders_instead_of_raising():
    """``scale = i32::MIN`` also used to render in Node and raise in Python."""
    rendered = cqlite._decimal_from_parts(-(2**31), b"\x01")
    assert rendered == Decimal("1e2147483648")
