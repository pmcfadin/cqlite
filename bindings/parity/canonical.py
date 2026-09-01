"""Canonical JSON form for 3-way cross-binding parity (issue #1455).

This module is HALF of a deliberately duplicated pair: ``canonical.py`` (this
file) and ``canonical.mjs``. The two are independent implementations of ONE
written specification (see ``bindings/parity/README.md``), and they are only
KNOWN to agree because ``canonical-vectors.json`` pins both against the same
expected output. Do not "simplify" one without re-running the vector suite.

The canonical form is a plain JSON value built from a DECLARED CQL type plus a
leg-specific value representation. Three adapters exist because the same row
arrives in three shapes:

  * ``PythonAdapter`` -- native values from the PyO3 binding
    (``uuid.UUID``, ``datetime``, ``bytes``, ``decimal.Decimal``, ...)
  * ``CliAdapter``    -- ``json.loads`` of ``cqlite --out json``
    (strings for temporal/blob/decimal/duration, numbers for ints)
  * (in ``canonical.mjs``) ``nodeAdapter`` -- native values from the napi
    binding (``Date``, ``BigInt``, ``Buffer``, ``Map``, ``Set``)

Canonicalization is TYPE-DRIVEN, never value-shape-driven: the declared CQL
type for every column lives in ``fixtures.json``. That is the no-heuristics
mandate (issue #28) applied to the harness itself -- a text column holding
``"2025-06-18"`` must never be re-interpreted as a date.
"""

from __future__ import annotations

import datetime as _dt
import re
from functools import cmp_to_key
from typing import Any, Callable, Iterable, List, NamedTuple, Sequence, Tuple

__all__ = [
    "CanonicalError",
    "CqlType",
    "parse_type",
    "types_from_columns",
    "subtree_has_udt",
    "canonical_compare",
    "canonical_sort_key",
    "canon_python",
    "canon_cli",
    "canon_row_python",
    "canon_row_cli",
    "normalize_decimal_string",
    "shape_tag",
    "canonical_equal",
    "JS_SAFE_INT_MAX",
]


class CanonicalError(Exception):
    """A value did not match its DECLARED type, or the type is unsupported.

    Always raised, never swallowed: a canonicalizer that silently guesses is
    exactly the heuristic this harness exists to rule out.
    """


# Integers outside JavaScript's exact-integer range canonicalize to a DECIMAL
# STRING rather than a JSON number, so the JS and Python legs cannot disagree
# about a value JS could not represent. Applied identically in all three
# adapters (README: "integer rule").
JS_SAFE_INT_MAX = 2**53 - 1

_INT_KINDS = frozenset(
    {"tinyint", "smallint", "int", "bigint", "counter", "varint"}
)
_TEXT_KINDS = frozenset({"text", "ascii", "varchar"})
_FLOAT_KINDS = frozenset({"float", "double"})
_UUID_KINDS = frozenset({"uuid", "timeuuid"})

_EPOCH = _dt.datetime(1970, 1, 1, tzinfo=_dt.timezone.utc)

_CLI_TIMESTAMP_RE = re.compile(
    r"^(-?\d{4,})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})\.(\d{3})\+0000$"
)
_CLI_DATE_RE = re.compile(r"^(-?\d{4,})-(\d{2})-(\d{2})$")
_CLI_TIME_RE = re.compile(r"^(\d{2}):(\d{2}):(\d{2})\.(\d{1,9})$")
_CLI_DURATION_RE = re.compile(
    r"^(?:(-?\d+)mo)?(?:(-?\d+)d)?(?:(-?\d+)ns)?$"
)
_HEX_RE = re.compile(r"^[0-9a-f]*$")
_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)
_DECIMAL_RE = re.compile(r"^(-?)(\d+)(?:\.(\d+))?(?:[eE]([+-]?\d+))?$")

# Above this many characters the plain (positional) decimal form is not
# materialized; the normalized exponent form is kept instead. Same constant and
# same rule in canonical.mjs -- an unbounded expansion would be a DoS on a
# pathological scale, and the two implementations must agree on the boundary.
DECIMAL_PLAIN_MAX_CHARS = 4096


# ---------------------------------------------------------------------------
# CQL type strings
# ---------------------------------------------------------------------------


class CqlType(NamedTuple):
    kind: str
    args: Tuple["CqlType", ...]

    def render(self) -> str:
        if not self.args:
            return self.kind
        return f"{self.kind}<{', '.join(a.render() for a in self.args)}>"


def _tokenize_type(text: str) -> List[str]:
    tokens: List[str] = []
    current = ""
    for ch in text:
        if ch in "<>,":
            if current.strip():
                tokens.append(current.strip())
            current = ""
            tokens.append(ch)
        else:
            current += ch
    if current.strip():
        tokens.append(current.strip())
    return tokens


def _parse_tokens(tokens: Sequence[str], i: int) -> Tuple[CqlType, int]:
    if i >= len(tokens):
        raise CanonicalError("truncated CQL type string")
    name = tokens[i].lower()
    if name in "<>,":
        raise CanonicalError(f"unexpected token {name!r} where a type name was expected")
    i += 1
    args: List[CqlType] = []
    if i < len(tokens) and tokens[i] == "<":
        i += 1
        while True:
            node, i = _parse_tokens(tokens, i)
            args.append(node)
            if i >= len(tokens):
                raise CanonicalError("unbalanced '<' in CQL type string")
            if tokens[i] == ",":
                i += 1
                continue
            if tokens[i] == ">":
                i += 1
                break
            raise CanonicalError(f"unexpected token {tokens[i]!r} in CQL type string")
    if name == "frozen":
        if len(args) != 1:
            raise CanonicalError("frozen<> takes exactly one argument")
        return args[0], i
    return CqlType(name, tuple(args)), i


def parse_type(text: str) -> CqlType:
    """Parse a declared CQL type string into a type tree.

    ``frozen<X>`` is transparent -- it is a storage property, not a value
    shape, and none of the three legs surfaces it.
    """
    tokens = _tokenize_type(text)
    if not tokens:
        raise CanonicalError("empty CQL type string")
    node, i = _parse_tokens(tokens, 0)
    if i != len(tokens):
        raise CanonicalError(f"trailing tokens in CQL type string: {text!r}")
    _validate_arity(node)
    return node


def _validate_arity(t: CqlType) -> None:
    if t.kind in ("list", "set"):
        if len(t.args) != 1:
            raise CanonicalError(f"{t.kind}<> takes exactly one argument")
    elif t.kind == "map":
        if len(t.args) != 2:
            raise CanonicalError("map<> takes exactly two arguments")
    elif t.kind == "tuple":
        if not t.args:
            raise CanonicalError("tuple<> takes at least one argument")
    elif t.args:
        raise CanonicalError(f"type {t.kind!r} does not take type arguments")
    elif t.kind not in _SCALAR_KINDS:
        # DECLARED GAP: user-defined types are not canonicalizable here without
        # their declared field types, and inferring them from the value would
        # be the heuristic issue #28 forbids. Refuse loudly.
        raise CanonicalError(
            f"unsupported CQL type {t.kind!r} in the parity harness "
            "(UDTs and unlisted scalars are a declared gap -- see "
            "bindings/parity/README.md)"
        )
    for a in t.args:
        _validate_arity(a)


_SCALAR_KINDS = (
    _INT_KINDS
    | _TEXT_KINDS
    | _FLOAT_KINDS
    | _UUID_KINDS
    | frozenset({"boolean", "blob", "timestamp", "date", "time", "duration", "decimal", "inet"})
)


# ---------------------------------------------------------------------------
# Total order over canonical values
# ---------------------------------------------------------------------------


def _pytype(v: Any) -> str:
    return type(v).__name__


def _rank(v: Any) -> int:
    if v is None:
        return 0
    if isinstance(v, bool):
        return 1
    if isinstance(v, (int, float)):
        return 2
    if isinstance(v, str):
        return 3
    if isinstance(v, list):
        return 4
    if isinstance(v, dict):
        return 5
    raise CanonicalError(f"not a canonical value: {type(v).__name__}")


def _cmp(a: Any, b: Any) -> int:
    return (a > b) - (a < b)


def canonical_compare(a: Any, b: Any) -> int:
    """Total order over CANONICAL values, identical to canonicalCompare() in JS.

    Used to sort sets and map entries so that an unordered collection has ONE
    representation on all three legs. Strings compare by Unicode CODE POINT
    (Python's native ``<``); the JS twin re-implements that explicitly, because
    JS ``<`` compares UTF-16 code units and would order astral characters
    differently.
    """
    ra, rb = _rank(a), _rank(b)
    if ra != rb:
        return _cmp(ra, rb)
    if ra in (0,):
        return 0
    if ra in (1, 2, 3):
        return _cmp(a, b)
    if ra == 4:
        for x, y in zip(a, b):
            c = canonical_compare(x, y)
            if c:
                return c
        return _cmp(len(a), len(b))
    ka, kb = sorted(a.keys()), sorted(b.keys())
    c = canonical_compare(ka, kb)
    if c:
        return c
    for k in ka:
        c = canonical_compare(a[k], b[k])
        if c:
            return c
    return 0


canonical_sort_key = cmp_to_key(canonical_compare)


# ---------------------------------------------------------------------------
# Shared scalar canonicalizers
# ---------------------------------------------------------------------------


def canon_int(n: int) -> Any:
    if not isinstance(n, int) or isinstance(n, bool):
        raise CanonicalError(f"expected an integer, got {type(n).__name__}")
    return n if abs(n) <= JS_SAFE_INT_MAX else str(n)


def canon_hex(data: bytes) -> str:
    return "0x" + data.hex()


def canon_uuid_str(s: str) -> str:
    low = s.lower()
    if not _UUID_RE.match(low):
        raise CanonicalError(f"not a hyphenated UUID: {s!r}")
    return low


def normalize_decimal_string(s: str) -> str:
    """Canonical plain-decimal text, identical to normalizeDecimalString() in JS.

    Trailing zeros of the SCALE are preserved (Cassandra's decimal carries a
    scale, and ``0.10`` is not ``0.1`` on the wire). Only the exponent is
    normalized away, and only when the positional form stays bounded.
    """
    text = str(s).strip()
    m = _DECIMAL_RE.match(text)
    if not m:
        raise CanonicalError(f"not a decimal string: {s!r}")
    sign, int_part, frac_part, exp_part = m.group(1), m.group(2), m.group(3) or "", m.group(4)
    exp = int(exp_part) if exp_part is not None else 0
    digits = int_part + frac_part
    point = len(int_part) + exp  # index of the decimal point within `digits`
    if abs(point) > DECIMAL_PLAIN_MAX_CHARS or len(digits) > DECIMAL_PLAIN_MAX_CHARS:
        stripped = digits.lstrip("0") or "0"
        scale = len(frac_part) - exp
        body = f"{sign}{stripped}"
        return body if scale == 0 else f"{body}e{-scale}"
    if point <= 0:
        digits = "0" * (1 - point) + digits
        point = 1
    elif point > len(digits):
        digits = digits + "0" * (point - len(digits))
    whole = digits[:point].lstrip("0") or "0"
    frac = digits[point:]
    out = whole if not frac else f"{whole}.{frac}"
    # A negative zero keeps its sign, matching every leg's own rendering.
    return f"{sign}{out}"


def shape_tag(v: Any) -> str:
    """Type-tagged shape of a CANONICAL value -- ONE definition, every caller.

    ``int`` and ``float`` deliberately collapse to ``"number"``. JSON has a
    single number type and the Node leg crosses a JSON boundary
    (``JSON.stringify({h: 1.0})`` -> ``{"h":1}``, which ``json.load`` returns as
    ``int``), so an INTEGRAL double would report ``float`` on the python/cli
    legs and ``int`` on the node leg for an IDENTICAL canonical value. That is a
    false red on correct input -- latent only because today's corpus happens to
    hold no integral float -- and a lane that reds on correct input is the lane
    agents learn to waive. ``bool`` is tested FIRST, so the property this tag
    actually enforces (number vs string vs bool vs null) is unaffected: the
    integer rule's above-2^53 case is number-vs-STRING, which still fails.
    """
    if isinstance(v, bool):
        return "bool"
    if isinstance(v, (int, float)):
        return "number"
    if isinstance(v, str):
        return "str"
    if v is None:
        return "null"
    if isinstance(v, list):
        return "[" + ",".join(shape_tag(x) for x in v) + "]"
    if isinstance(v, dict):
        return "{" + ",".join(f"{k}:{shape_tag(v[k])}" for k in sorted(v)) + "}"
    return type(v).__name__


def canonical_equal(a: Any, b: Any) -> bool:
    """Equality over canonical values that ALSO compares :func:`shape_tag`.

    A bare ``==`` in Python makes ``1 == True`` and ``1 == 1.0``; only the first
    of those is a real divergence, and the shape tag is what separates them.
    """
    return a == b and shape_tag(a) == shape_tag(b)


def _canon_duration(months: int, days: int, nanos: int) -> dict:
    return {
        "months": canon_int(int(months)),
        "days": canon_int(int(days)),
        "nanos": canon_int(int(nanos)),
    }


# ---------------------------------------------------------------------------
# Adapters
# ---------------------------------------------------------------------------


class _Adapter:
    name = "?"

    def as_seq(self, value: Any, t: "CqlType", hashable: bool = False) -> Iterable[Any]:
        raise NotImplementedError

    def as_map(
        self, value: Any, t: "CqlType", hashable: bool = False
    ) -> Iterable[Tuple[Any, Any]]:
        raise NotImplementedError

    def scalar(self, value: Any, kind: str) -> Any:
        raise NotImplementedError


class PythonAdapter(_Adapter):
    """Native values from the PyO3 binding.

    The container check is TYPE-SPECIFIC (issue #1455, F4). Accepting list,
    tuple and set interchangeably normalized away exactly the regression this
    harness exists to catch -- a binding returning an ``Array``/``list`` for a
    declared ``set<...>``, or a ``Set`` for a declared ``list<...>``, is a
    change to a public API shape and must RED, not be silently reconciled.

    THREE intentional projections are allowed, each derived from the binding's
    own source rather than from CQLite's prior behaviour:

    1. ``set<...>`` whose element subtree contains a UDT arrives as a ``list``
       (#804/#3500). Measured: ``bindings/python/src/value.rs::set_to_py``
       branches on ``items.iter().any(contains_udt)`` -- UDT-containment, NOT
       unhashability. Currently UNREACHABLE here, because ``parse_type``
       refuses UDT type names outright; it is implemented and tested so that
       adding UDT support cannot silently turn a correct binding red.
    2. Inside a HASHABLE position -- a ``set`` element or a ``map`` KEY --
       every container is projected by
       ``bindings/python/src/value_hashable.rs::value_to_hashable_key``:
       ``list``/``tuple`` become a Python ``tuple``, ``set`` stays a
       ``frozenset``, and ``map`` becomes a ``tuple`` of 2-``tuple``s. So a
       ``set<frozen<list<int>>>`` is a ``frozenset`` OF ``tuple``s, not of
       lists, and requiring a ``list`` there would red on correct input.
    3. The #804 ``list`` allowance applies ONLY outside a hashable position:
       that same source comment records that recursion inside
       ``value_to_hashable_key`` never re-enters ``set_to_py``, so the UDT
       branch is unreachable there.
    """

    name = "python"

    def as_seq(self, value: Any, t: CqlType, hashable: bool = False) -> Iterable[Any]:
        kind = t.kind
        if kind == "list":
            if hashable:
                # Projection 2: hashable positions carry a tuple.
                if isinstance(value, tuple):
                    return list(value)
                raise CanonicalError(
                    "declared list<> in a hashable position (set element / map key) expects a "
                    f"Python tuple — value_hashable.rs projects it — got {_pytype(value)}"
                )
            if isinstance(value, list):
                return value
            raise CanonicalError(f"declared list<> expects a Python list, got {_pytype(value)}")
        if kind == "tuple":
            if isinstance(value, tuple):
                return list(value)
            raise CanonicalError(f"declared tuple<> expects a Python tuple, got {_pytype(value)}")
        if kind == "set":
            if isinstance(value, (frozenset, set)):
                return list(value)
            if not hashable and isinstance(value, list) and subtree_has_udt(t):
                # Projection 1 (#804/#3500): SET<FROZEN<UDT>> is a list.
                return value
            raise CanonicalError(
                f"declared set<> expects a Python frozenset/set, got {_pytype(value)}"
            )
        raise CanonicalError(f"as_seq called for non-sequence kind {kind!r}")

    def as_map(self, value: Any, t: CqlType, hashable: bool = False) -> Iterable[Tuple[Any, Any]]:
        if hashable:
            # Projection 2: a map inside a hashable position is a tuple of
            # 2-tuples (value_hashable.rs), never a dict.
            if isinstance(value, tuple) and all(
                isinstance(pair, tuple) and len(pair) == 2 for pair in value
            ):
                return list(value)
            raise CanonicalError(
                "declared map<> in a hashable position (set element / map key) expects a Python "
                f"tuple of (key, value) tuples — value_hashable.rs projects it — got "
                f"{_pytype(value)}"
            )
        if isinstance(value, dict):
            return list(value.items())
        raise CanonicalError(f"declared map<> expects a Python dict, got {_pytype(value)}")

    def scalar(self, value: Any, kind: str) -> Any:
        import decimal as _decimal
        import ipaddress as _ip
        import uuid as _uuidmod

        if kind == "boolean":
            if not isinstance(value, bool):
                raise CanonicalError(f"boolean column got {type(value).__name__}")
            return value
        if kind in _INT_KINDS:
            return canon_int(value)
        if kind in _FLOAT_KINDS:
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise CanonicalError(f"{kind} column got {type(value).__name__}")
            return float(value)
        if kind in _TEXT_KINDS:
            if not isinstance(value, str):
                raise CanonicalError(f"{kind} column got {type(value).__name__}")
            return value
        if kind == "blob":
            if not isinstance(value, (bytes, bytearray, memoryview)):
                raise CanonicalError(f"blob column got {type(value).__name__}")
            return canon_hex(bytes(value))
        if kind in _UUID_KINDS:
            if not isinstance(value, _uuidmod.UUID):
                raise CanonicalError(f"{kind} column got {type(value).__name__}")
            return canon_uuid_str(str(value))
        if kind == "timestamp":
            if not isinstance(value, _dt.datetime):
                raise CanonicalError(f"timestamp column got {type(value).__name__}")
            dt = value if value.tzinfo else value.replace(tzinfo=_dt.timezone.utc)
            delta = dt.astimezone(_dt.timezone.utc) - _EPOCH
            return canon_int(
                delta.days * 86_400_000 + delta.seconds * 1000 + delta.microseconds // 1000
            )
        if kind == "date":
            if isinstance(value, _dt.datetime) or not isinstance(value, _dt.date):
                raise CanonicalError(f"date column got {type(value).__name__}")
            return value.isoformat()
        if kind == "time":
            return canon_int(value)
        if kind == "duration":
            months = getattr(value, "months", None)
            days = getattr(value, "days", None)
            nanos = getattr(value, "nanos", None)
            if months is None or days is None or nanos is None:
                raise CanonicalError(f"duration column got {type(value).__name__}")
            return _canon_duration(months, days, nanos)
        if kind == "decimal":
            if not isinstance(value, _decimal.Decimal):
                raise CanonicalError(f"decimal column got {type(value).__name__}")
            # `str()`, NEVER `format(value, "f")`: the "f" presentation expands the
            # scale to POSITIONAL notation before normalize_decimal_string's
            # DECIMAL_PLAIN_MAX_CHARS guard can refuse it, so a Decimal carrying a
            # pathological 32-bit exponent attempts a multi-gigabyte allocation
            # inside format() itself. str() is bounded (Decimal renders scientific
            # for extreme exponents) and the guard then decides (issue #1455, R2).
            return normalize_decimal_string(str(value))
        if kind == "inet":
            if not isinstance(value, (_ip.IPv4Address, _ip.IPv6Address)):
                raise CanonicalError(f"inet column got {type(value).__name__}")
            return str(value)
        raise CanonicalError(f"unsupported scalar kind {kind!r}")


class CliAdapter(_Adapter):
    """``json.loads`` of ``cqlite --out json`` output."""

    name = "cli"

    def as_seq(self, value: Any, t: CqlType, hashable: bool = False) -> Iterable[Any]:
        # DECLARED, and NOT a hole this fix can close: the CLI renders list,
        # set AND tuple all as a bare JSON array
        # (cqlite-cli/src/output/json.rs), so this leg cannot distinguish the
        # three at all. F4's type-specific container check is therefore
        # enforceable on the python and node legs ONLY; here the check is just
        # "is it an array". See README gap 1.
        if isinstance(value, list):
            return value
        raise CanonicalError(
            f"declared {t.kind}<> expects a JSON array, got {type(value).__name__}"
        )

    def as_map(self, value: Any, t: CqlType, hashable: bool = False) -> Iterable[Tuple[Any, Any]]:
        # The CLI renders a map as [{"key": k, "value": v}, ...], at every
        # nesting depth -- there is no hashable-position projection on this leg.
        if not isinstance(value, list):
            raise CanonicalError(
                f"declared map<> expects a JSON array of entries, got {type(value).__name__}"
            )
        out = []
        for entry in value:
            if not isinstance(entry, dict) or set(entry.keys()) != {"key", "value"}:
                raise CanonicalError(f"malformed CLI map entry: {entry!r}")
            out.append((entry["key"], entry["value"]))
        return out

    def scalar(self, value: Any, kind: str) -> Any:
        if kind == "boolean":
            if not isinstance(value, bool):
                raise CanonicalError(f"boolean column got {type(value).__name__}")
            return value
        if kind == "varint":
            if not isinstance(value, str):
                raise CanonicalError(f"varint column got {type(value).__name__}")
            return canon_int(int(value))
        if kind in _INT_KINDS:
            return canon_int(value)
        if kind in _FLOAT_KINDS:
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise CanonicalError(f"{kind} column got {type(value).__name__}")
            return float(value)
        if kind in _TEXT_KINDS:
            if not isinstance(value, str):
                raise CanonicalError(f"{kind} column got {type(value).__name__}")
            return value
        if kind == "blob":
            if not isinstance(value, str) or not value.startswith("0x"):
                raise CanonicalError(f"blob column got {value!r}")
            body = value[2:].lower()
            if not _HEX_RE.match(body):
                raise CanonicalError(f"blob column is not hex: {value!r}")
            return "0x" + body
        if kind in _UUID_KINDS:
            if not isinstance(value, str):
                raise CanonicalError(f"{kind} column got {type(value).__name__}")
            return canon_uuid_str(value)
        if kind == "timestamp":
            m = _CLI_TIMESTAMP_RE.match(value if isinstance(value, str) else "")
            if not m:
                raise CanonicalError(f"unparseable CLI timestamp: {value!r}")
            y, mo, d, h, mi, s, ms = (int(g) for g in m.groups())
            dt = _dt.datetime(y, mo, d, h, mi, s, tzinfo=_dt.timezone.utc)
            delta = dt - _EPOCH
            return canon_int(delta.days * 86_400_000 + delta.seconds * 1000 + ms)
        if kind == "date":
            if not isinstance(value, str) or not _CLI_DATE_RE.match(value):
                raise CanonicalError(f"unparseable CLI date: {value!r}")
            return value
        if kind == "time":
            m = _CLI_TIME_RE.match(value if isinstance(value, str) else "")
            if not m:
                raise CanonicalError(f"unparseable CLI time: {value!r}")
            h, mi, s, frac = m.group(1), m.group(2), m.group(3), m.group(4)
            nanos = (int(h) * 3600 + int(mi) * 60 + int(s)) * 1_000_000_000
            nanos += int(frac.ljust(9, "0"))
            return canon_int(nanos)
        if kind == "duration":
            if not isinstance(value, str) or not value:
                raise CanonicalError(f"unparseable CLI duration: {value!r}")
            m = _CLI_DURATION_RE.match(value)
            if not m or not any(m.groups()):
                raise CanonicalError(f"unparseable CLI duration: {value!r}")
            months, days, nanos = (int(g) if g is not None else 0 for g in m.groups())
            return _canon_duration(months, days, nanos)
        if kind == "decimal":
            if not isinstance(value, str):
                raise CanonicalError(f"decimal column got {type(value).__name__}")
            return normalize_decimal_string(value)
        if kind == "inet":
            if not isinstance(value, str):
                raise CanonicalError(f"inet column got {type(value).__name__}")
            return value
        raise CanonicalError(f"unsupported scalar kind {kind!r}")


PYTHON_ADAPTER = PythonAdapter()
CLI_ADAPTER = CliAdapter()


# ---------------------------------------------------------------------------
# Type-driven walk
# ---------------------------------------------------------------------------


def subtree_has_udt(t: CqlType) -> bool:
    """True when a UDT appears anywhere in ``t``'s type tree (#804/#3500).

    Currently always False in practice, because :func:`parse_type` REFUSES a
    UDT type name (see ``_validate_arity``). It exists so that the #804
    ``SET<FROZEN<UDT>>`` -> ``list`` projection is already allowed for on the
    day UDT support lands, instead of the container check turning a correct
    binding red. ``test_set_of_udt_projection_is_allowed`` builds the type tree
    directly to keep this branch live and tested.
    """
    if t.kind == "udt":
        return True
    return any(subtree_has_udt(a) for a in t.args)


def _canon(value: Any, t: CqlType, ad: _Adapter, hashable: bool = False) -> Any:
    """``hashable`` marks a SET-ELEMENT or MAP-KEY position.

    The Python binding projects every container inside such a position through
    ``value_hashable.rs``; the node and cli legs do not (measured:
    ``bindings/node/src/value.rs`` recurses through ``value_to_napi``
    unconditionally, and the CLI writer has no key-specific path). Once set,
    the flag never clears -- ``value_to_hashable_key`` recurses into itself.
    """
    if value is None:
        return None
    kind = t.kind
    if kind == "list":
        return [_canon(x, t.args[0], ad, hashable) for x in ad.as_seq(value, t, hashable)]
    if kind == "set":
        # Elements of a set are in a HASHABLE position.
        items = [_canon(x, t.args[0], ad, True) for x in ad.as_seq(value, t, hashable)]
        items.sort(key=canonical_sort_key)
        return items
    if kind == "map":
        # KEYS are in a hashable position; VALUES are not (map_to_py projects
        # only the key through value_to_hashable_key).
        entries = [
            [_canon(k, t.args[0], ad, True), _canon(v, t.args[1], ad, hashable)]
            for k, v in ad.as_map(value, t, hashable)
        ]
        entries.sort(key=lambda e: canonical_sort_key(e[0]))
        return entries
    if kind == "tuple":
        # DECLARED GAP (README): a tuple canonicalizes to a PLAIN array, because
        # neither the Node binding nor the CLI can distinguish tuple from list.
        items = list(ad.as_seq(value, t, hashable))
        if len(items) != len(t.args):
            raise CanonicalError(
                f"tuple arity mismatch: declared {len(t.args)}, value has {len(items)}"
            )
        return [_canon(x, t.args[i], ad, hashable) for i, x in enumerate(items)]
    return ad.scalar(value, kind)


def types_from_columns(columns: dict) -> dict:
    """Column name -> parsed :class:`CqlType`. ONE builder, every caller.

    Trivial in Python -- a ``dict`` has no prototype -- and it exists so the
    Python and JS halves have the SAME entry point for the row-building path,
    which the ``rows`` section of ``canonical-vectors.json`` pins in both. The
    JS twin (``typesFromColumns``) must build a NULL-PROTOTYPE object, because
    ``__proto__`` is a legal CQL column name and an ordinary object would
    silently swallow it (issue #1455, F1).
    """
    return {name: parse_type(text) for name, text in columns.items()}


def canon_python(value: Any, t: CqlType) -> Any:
    return _canon(value, t, PYTHON_ADAPTER)


def canon_cli(value: Any, t: CqlType) -> Any:
    return _canon(value, t, CLI_ADAPTER)


def _canon_row(row: Any, types: dict, ad: _Adapter, getter: Callable[[Any, str], Any]) -> dict:
    out = {}
    for name, t in types.items():
        try:
            out[name] = _canon(getter(row, name), t, ad)
        except CanonicalError as exc:
            raise CanonicalError(f"[{ad.name}] column {name!r} ({t.render()}): {exc}") from exc
    return out


def canon_row_python(row: Any, types: dict) -> dict:
    """Canonicalize one PyO3 ``Row`` (or dict). Absent column => JSON null."""

    def get(r: Any, name: str) -> Any:
        try:
            keys = r.keys()
        except AttributeError:
            keys = ()
        if name in list(keys):
            return r[name]
        return None

    return _canon_row(row, types, PYTHON_ADAPTER, get)


def canon_row_cli(row: dict, types: dict) -> dict:
    """Canonicalize one CLI JSON row object. Absent column => JSON null."""
    return _canon_row(row, types, CLI_ADAPTER, lambda r, name: r.get(name))
