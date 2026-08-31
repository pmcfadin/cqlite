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

* ``kind == "value"`` → the path must render, and the **full** rendering must
  satisfy the entry's exact check: ``rendered == expected`` for a short
  rendering, or ``sha256(rendered) == expected_sha256`` for a multi-kilobyte one
  whose literal form is committed as a digest. ``actual`` (the digest) is
  compared too, but only as the readable half of a failure message — a digest
  collapses a long digit run to ``{<length>}``, so on its own it would compare a
  digit COUNT and pass two bindings that render *different digits of the same
  length*.
* ``kind == "error"`` → the path must refuse, and ``expected`` must appear
  **verbatim inside** ``actual``. Containment only because each binding wraps the
  one canonical message in its own typed-error envelope.

The hash is SHA-256 over the **UTF-8 bytes** of the rendered string, lower-case
hex — the same statement the shared crate's ``vectors`` module makes, so this
suite, the Node suite and the crate's own test cannot disagree about encoding.
Each side hashes with its own standard library (``hashlib`` here, ``crypto`` in
Node, ``sha2`` in the crate): three independent implementations over one
committed hex string.
"""

from __future__ import annotations

import hashlib
import re
from decimal import Decimal

import pytest

import cqlite


def _sha256_hex(text: str) -> str:
    """Lower-case SHA-256 hex of a string's UTF-8 bytes."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _vectors():
    """The committed table, rendered through this binding's production paths."""
    return cqlite._ffi_common_render_vectors()


def _by_type(cql_type: str):
    return [entry for entry in _vectors() if entry["cql_type"] == cql_type]


def test_vector_table_is_present_and_covers_every_type():
    """A vacuous pass is impossible: the table must be non-empty per type.

    Without this, an empty table would make every parametrised assertion below
    trivially satisfied.
    """
    entries = _vectors()
    assert entries, "the shared vector table must not be empty"
    for cql_type in ("decimal", "varint", "inet", "json_number"):
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
        # The readable half.
        assert entry["actual"] == entry["expected"], entry["name"]
        # The EXACT half, on the full rendering this binding produced.
        rendered = entry["rendered"]
        assert isinstance(rendered, str), entry["name"]
        if entry["expected_sha256"] is None:
            assert rendered == entry["expected"], entry["name"]
        else:
            assert _sha256_hex(rendered) == entry["expected_sha256"], (
                f"{entry['name']}: the rendering digests to the expected "
                f"{entry['expected']!r} but its digits differ"
            )
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


def test_every_value_entry_carries_an_exact_oracle_not_just_a_digest():
    """No value entry may be checked by digit count alone.

    Either its expectation is committed verbatim (so equality is exact) or it
    carries the SHA-256 of the full rendering. Without this, a future long entry
    could quietly regress to the digest-only comparison this pairing exists to
    prevent: a digest pins the digit COUNT and the surrounding form, so two
    bindings rendering different digits of the same length would both pass.
    """
    values = [entry for entry in _vectors() if entry["kind"] == "value"]
    assert values
    digested = 0
    for entry in values:
        collapsed = "{" in entry["expected"]
        has_hash = entry["expected_sha256"] is not None
        assert collapsed == has_hash, entry["name"]
        if has_hash:
            assert re.fullmatch(r"[0-9a-f]{64}", entry["expected_sha256"]), entry["name"]
            digested += 1
    # The multi-kilobyte boundary magnitudes are the reason this exists.
    assert digested >= 3


def test_the_digested_renderings_match_the_committed_hashes_digit_for_digit():
    """The digested entries, hashed here with ``hashlib`` from this binding's own
    rendering — the check the Node suite makes with ``crypto`` over the same
    committed hex."""
    digested = [
        entry
        for entry in _by_type("decimal")
        if entry["kind"] == "value" and entry["expected_sha256"] is not None
    ]
    assert len(digested) >= 3
    for entry in digested:
        assert _sha256_hex(entry["rendered"]) == entry["expected_sha256"], entry["name"]
    convergence = next(
        entry
        for entry in digested
        if entry["name"] == "decimal/large-well-formed-2000-bytes-scale-3"
    )
    # Every one of the 4817 digits, not just how many there are.
    assert re.fullmatch(r"[0-9]{4817}e-3", convergence["rendered"])
    assert (
        convergence["expected_sha256"]
        == "e1ec7b41fe833049052e89e01d3cdda36fcfc6dd69ec5deb03d52c116aa55214"
    )


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


# =============================================================================
# JSON numbers (issue #3505) — the WIRING evidence for `json_number_to_py`
# =============================================================================
#
# Why these exist rather than only the shared-crate tests: before this table the
# production adapter `value::json_number_to_py` had ZERO test callers anywhere in
# the repository. The mutation `JsonNumberClass::U64(u) => (u as f64)` reddened
# NOTHING — not the shared crate's own tests (they pin `classify_json_number`
# only), not `test_json_number_precision.py` (it pins the comparison harness),
# not jest. So AC1's observable claim ("`u64::MAX` reaches Python as an exact
# `int`") was asserted by no test at all. `cqlite-ffi-common/src/vectors.rs` says
# it in general: the shared tests "do NOT prove a binding actually CALLS them".
#
# Both halves are needed. The rendered TEXT catches a value change; the HOST TYPE
# catches a same-text type change, which `str()` cannot see.


@pytest.mark.parametrize("entry", _by_type("json_number"), ids=lambda e: e["name"])
def test_json_number_arrives_as_the_committed_host_type(entry):
    """The host-shape half, through the full production dispatch.

    ``_json_number_from_text`` drives
    ``value_to_py`` → ``json_to_py`` → ``json_number_to_py`` → the shared
    classifier, i.e. exactly the chain a real result row takes.
    """
    assert entry["kind"] == "value", (
        f"{entry['name']}: the JSON-number table commits no refusals "
        "(the Beyond arm is unreachable in a default build)"
    )
    text = entry["bytes"].decode("utf-8")
    value = cqlite._json_number_from_text(text)

    if entry["host_kind"] == "integer":
        # `type(...) is int` deliberately, not `isinstance`: a lossy arm returns
        # a `float`, and `isinstance(True, int)` would also admit a bool.
        assert type(value) is int, (
            f"{entry['name']}: `{text}` arrived as {type(value).__name__} "
            f"({value!r}); an integer literal must never become a float"
        )
        assert value == int(entry["expected"]), entry["name"]
    elif entry["host_kind"] == "float":
        assert type(value) is float, (
            f"{entry['name']}: `{text}` arrived as {type(value).__name__}"
        )
        assert value == float(entry["expected"]), entry["name"]
    else:  # pragma: no cover - fail closed on an unknown host kind
        pytest.fail(f"{entry['name']}: unknown host_kind {entry['host_kind']!r}")


def test_the_u64_range_is_actually_covered_by_the_json_number_table():
    """The table must contain the class #3505 was losing, not just easy cases.

    A guard against the table being silently narrowed to values an f64 can hold,
    which would leave every assertion above green while covering nothing.
    """
    entries = _by_type("json_number")
    above_i64_max = [
        e for e in entries
        if e["host_kind"] == "integer" and int(e["expected"]) > 2**63 - 1
    ]
    assert above_i64_max, (
        "no JSON-number vector exceeds i64::MAX — the #3505 class is uncovered"
    )
    # And at least one whose f64 rounding is OBSERVABLE in the rendering, so the
    # text half of the check is not relying on the type half alone.
    assert any(
        str(float(int(e["expected"]))) != e["expected"] for e in above_i64_max
    ), "no covered value actually loses digits through an f64"


def test_json_number_from_text_is_fail_closed_on_non_numbers():
    """A typo'd literal must raise, never render a substituted default."""
    for bad in ('"18446744073709551615"', "not-a-number", "", "[1]", "1 2"):
        with pytest.raises(ValueError):
            cqlite._json_number_from_text(bad)
