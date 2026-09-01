"""Differential pin for the Python canonicalizer (issue #1455).

``canonical.py`` and ``canonical.mjs`` are two independent implementations of
one written spec. This module materializes every vector in
``canonical-vectors.json`` into the PYTHON binding's native value shape (and
the CLI's parsed-JSON shape) and checks both against the vector's expected
canonical output; it also drives the ``errors`` table, which pins that each
canonicalizer REFUSES malformed input rather than guessing. ``vectors.mjs``
does the same for the Node shape, against the SAME file.

Runnable standalone::

    python bindings/parity/vectors.py
"""

from __future__ import annotations

import datetime as _dt
import decimal as _decimal
import ipaddress as _ipaddress
import json
import sys
import uuid as _uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from canonical import (  # noqa: E402
    CanonicalError,
    canon_cli,
    canon_python,
    canon_row_cli,
    canon_row_python,
    canonical_equal,
    parse_type,
    shape_tag,
    types_from_columns,
)

VECTORS_PATH = _HERE / "canonical-vectors.json"
_EPOCH = _dt.datetime(1970, 1, 1, tzinfo=_dt.timezone.utc)

# The legs this runner owns. The `node` leg is vectors.mjs's job.
LEGS = ("python", "cli")


def load_all(path: Path = VECTORS_PATH) -> dict:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def load_vectors(path: Path = VECTORS_PATH) -> List[dict]:
    return load_all(path)["vectors"]


def materialize_python(spec: Any) -> Any:
    """Turn a vector's leg spec into a PyO3-binding-shaped native value."""
    if spec is None or isinstance(spec, (bool, int, float, str)):
        return spec
    if isinstance(spec, list):
        return [materialize_python(x) for x in spec]
    if not isinstance(spec, dict) or "$" not in spec:
        raise ValueError(f"untagged vector spec: {spec!r}")
    tag = spec["$"]
    if tag == "uuid":
        return _uuid.UUID(spec["v"])
    if tag == "bytes":
        return bytes.fromhex(spec["hex"])
    if tag == "datetime":
        return _EPOCH + _dt.timedelta(milliseconds=spec["ms"])
    if tag == "date":
        return _dt.date.fromisoformat(spec["v"])
    if tag == "bigint":
        return int(spec["v"])
    if tag == "decimal":
        return _decimal.Decimal(spec["v"])
    if tag == "inet":
        return _ipaddress.ip_address(spec["v"])
    if tag == "duration":
        import cqlite

        return cqlite.Duration(spec["months"], spec["days"], int(spec["nanos"]))
    if tag == "list":
        return [materialize_python(x) for x in spec["items"]]
    if tag == "tuple":
        return tuple(materialize_python(x) for x in spec["items"])
    if tag == "set":
        return frozenset(materialize_python(x) for x in spec["items"])
    if tag == "map":
        return {materialize_python(k): materialize_python(v) for k, v in spec["entries"]}
    raise ValueError(f"unknown vector tag: {tag!r}")


# ---------------------------------------------------------------------------
# Value vectors
# ---------------------------------------------------------------------------


def check_vectors(vectors: Optional[List[dict]] = None) -> Tuple[List[str], Dict[str, int]]:
    """Return ``(failures, counts)``.

    ``counts`` reports ``checks`` (vector x leg pairs actually exercised),
    ``ok``, ``failed`` and ``skipped`` — per PAIR, not per vector, because a
    vector can fail on one leg and pass on the other and a per-vector tally
    then under-counts (issue #1455, N7).
    """
    failures: List[str] = []
    counts = {"vectors": 0, "checks": 0, "ok": 0, "failed": 0, "skipped": 0}
    for vec in vectors if vectors is not None else load_vectors():
        counts["vectors"] += 1
        expected = vec["canonical"]
        for leg in LEGS:
            if leg == "cli" and vec.get("cli") is None and expected is not None:
                # An explicitly "unreachable from the CLI" vector; python-only.
                counts["skipped"] += 1
                continue
            counts["checks"] += 1
            try:
                # parse_type is INSIDE the try (N6): a malformed `type` must be
                # reported as this vector's failure, not abort the whole sweep
                # and leave every later vector unmeasured.
                t = parse_type(vec["type"])
                value = materialize_python(vec[leg]) if leg == "python" else vec["cli"]
                actual = canon_python(value, t) if leg == "python" else canon_cli(value, t)
            except Exception as exc:  # noqa: BLE001 - report, do not abort the sweep
                failures.append(f"{vec['name']}/{leg}: raised {type(exc).__name__}: {exc}")
                counts["failed"] += 1
                continue
            if canonical_equal(actual, expected):
                counts["ok"] += 1
                continue
            failures.append(
                f"{vec['name']}/{leg}: expected {expected!r} ({shape_tag(expected)}), "
                f"got {actual!r} ({shape_tag(actual)})"
            )
            counts["failed"] += 1
    return failures, counts


# ---------------------------------------------------------------------------
# Row cases -- the ROW-BUILDING path (issue #1455, F1)
# ---------------------------------------------------------------------------


def materialize_python_row(spec: dict) -> dict:
    """A row dict, keys VERBATIM. Python dicts have no prototype to pollute."""
    return {name: materialize_python(value) for name, value in spec.items()}


def check_rows(rows: Optional[List[dict]] = None) -> Tuple[List[str], Dict[str, int]]:
    """Drive ``types_from_columns`` + ``canon_row_*`` for whole rows.

    The value vectors above never build a COLUMN-NAME-KEYED object, so they
    cannot reach the hazard this section exists for: ``__proto__`` is a legal
    quoted CQL identifier, and on an ordinary JS object assigning it replaces
    the prototype instead of creating an own property -- silently dropping the
    column from both the type map and the canonical row. Python is immune
    (dicts have no prototype); the case is driven here too so both halves are
    pinned against the SAME expected row rather than each against itself.
    """
    if rows is None:
        rows = load_all().get("rows", [])
    failures: List[str] = []
    counts = {"rows": 0, "checks": 0, "ok": 0, "failed": 0, "skipped": 0}
    for case in rows:
        counts["rows"] += 1
        expected = case["canonical"]
        for leg in LEGS:
            if leg not in case:
                counts["skipped"] += 1
                continue
            counts["checks"] += 1
            try:
                types = types_from_columns(case["columns"])
                if leg == "python":
                    actual = canon_row_python(materialize_python_row(case[leg]), types)
                else:
                    actual = canon_row_cli(case[leg], types)
            except Exception as exc:  # noqa: BLE001 - report, do not abort the sweep
                failures.append(f"{case['name']}/{leg}: raised {type(exc).__name__}: {exc}")
                counts["failed"] += 1
                continue
            missing = [c for c in case["columns"] if c not in actual]
            if missing:
                failures.append(
                    f"{case['name']}/{leg}: canonical row LOST column(s) {missing} "
                    f"(got keys {sorted(actual)})"
                )
                counts["failed"] += 1
                continue
            if canonical_equal(actual, expected):
                counts["ok"] += 1
                continue
            failures.append(
                f"{case['name']}/{leg}: expected {expected!r} ({shape_tag(expected)}), "
                f"got {actual!r} ({shape_tag(actual)})"
            )
            counts["failed"] += 1
    return failures, counts


# ---------------------------------------------------------------------------
# Refusal cases (issue #1455, N3)
# ---------------------------------------------------------------------------


def check_errors(errors: Optional[List[dict]] = None) -> Tuple[List[str], Dict[str, int]]:
    """Every entry must RAISE, and the message must name the reason.

    A canonicalizer that silently guesses on malformed input is the heuristic
    issue #28 forbids, so "it refused" is a pinned property, not a hope.
    """
    if errors is None:
        errors = load_all().get("errors", [])
    failures: List[str] = []
    counts = {"cases": 0, "checks": 0, "ok": 0, "failed": 0}
    for case in errors:
        counts["cases"] += 1
        expect = case["expect"]
        if case["stage"] == "parse_type":
            counts["checks"] += 1
            ok, detail = _expect_raise(lambda: parse_type(case["type"]), expect)
            _record(failures, counts, f"{case['name']}/parse_type", ok, detail)
            continue
        for leg, spec in case.get("legs", {}).items():
            if leg not in LEGS:
                continue
            counts["checks"] += 1
            ok, detail = _expect_raise(
                lambda leg=leg, spec=spec: (
                    canon_python(materialize_python(spec), parse_type(case["type"]))
                    if leg == "python"
                    else canon_cli(spec, parse_type(case["type"]))
                ),
                expect,
            )
            _record(failures, counts, f"{case['name']}/{leg}", ok, detail)
    return failures, counts


def _expect_raise(fn, expect: str) -> Tuple[bool, str]:
    try:
        result = fn()
    except CanonicalError as exc:
        if expect.lower() in str(exc).lower():
            return True, ""
        return False, f"raised CanonicalError but message lacks {expect!r}: {exc}"
    except Exception as exc:  # noqa: BLE001
        return False, f"raised {type(exc).__name__} (expected CanonicalError): {exc}"
    return False, f"did NOT raise; returned {result!r}"


def _record(failures: List[str], counts: Dict[str, int], label: str, ok: bool, detail: str) -> None:
    if ok:
        counts["ok"] += 1
    else:
        counts["failed"] += 1
        failures.append(f"{label}: {detail}")


# ---------------------------------------------------------------------------
# Case floor (issue #1455, B2)
# ---------------------------------------------------------------------------


def collect_kinds(type_text: str) -> List[str]:
    def walk(t) -> List[str]:
        out = [t.kind]
        for a in t.args:
            out.extend(walk(a))
        return out

    return walk(parse_type(type_text))


def check_floor(data: Optional[dict] = None) -> List[str]:
    """The subject set must not be able to shrink to nothing and stay green.

    #3544's own lesson, applied to this table: an empty or truncated
    ``canonical-vectors.json`` would otherwise report ``0/0 vectors OK``.
    """
    if data is None:
        data = load_all()
    floor = data.get("floor")
    if not isinstance(floor, dict):
        return ["canonical-vectors.json has no `floor` block — the case floor cannot be checked"]
    failures: List[str] = []
    vectors = data.get("vectors", [])
    errors = data.get("errors", [])
    if len(vectors) < floor["min_vectors"]:
        failures.append(
            f"vector floor: {len(vectors)} < {floor['min_vectors']} — vectors were REMOVED"
        )
    if len(errors) < floor["min_errors"]:
        failures.append(
            f"error-case floor: {len(errors)} < {floor['min_errors']} — cases were REMOVED"
        )
    rows = data.get("rows", [])
    if len(rows) < floor.get("min_rows", 0):
        failures.append(
            f"row-case floor: {len(rows)} < {floor['min_rows']} — row cases were REMOVED"
        )
    row_names = [r["name"] for r in rows]
    missing_rows = [n for n in floor.get("required_row_names", []) if n not in row_names]
    if missing_rows:
        failures.append(f"required row case(s) absent: {missing_rows}")
    names = [v["name"] for v in vectors]
    if len(set(names)) != len(names):
        failures.append("duplicate vector names")

    seen: set = set()
    nested = False
    for vec in vectors:
        try:
            kinds = collect_kinds(vec["type"])
        except CanonicalError as exc:
            failures.append(f"{vec['name']}: unparseable type {vec['type']!r}: {exc}")
            continue
        seen.update(kinds)
        containers = [k for k in kinds if k in ("list", "set", "map", "tuple")]
        if len(containers) >= 2:
            nested = True
    missing = [k for k in floor["required_kinds"] if k not in seen]
    if missing:
        failures.append(f"no vector covers CQL kind(s): {missing}")
    if floor.get("require_nested_container") and not nested:
        failures.append("no vector nests a container inside a container")
    if floor.get("require_null_canonical") and not any(v["canonical"] is None for v in vectors):
        failures.append("no vector canonicalizes to null")
    return failures


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> int:
    data = load_all()
    floor_failures = check_floor(data)
    vec_failures, vec_counts = check_vectors(data["vectors"])
    row_failures, row_counts = check_rows(data.get("rows", []))
    err_failures, err_counts = check_errors(data.get("errors", []))
    for line in floor_failures + vec_failures + row_failures + err_failures:
        print(f"FAIL {line}", file=sys.stderr)
    print(
        f"vectors: {vec_counts['ok']}/{vec_counts['checks']} leg-checks OK over "
        f"{vec_counts['vectors']} vectors "
        f"({vec_counts['skipped']} leg-checks skipped: cli-unreachable) "
        f"[legs: {', '.join(LEGS)}]"
    )
    print(
        f"refusals: {err_counts['ok']}/{err_counts['checks']} leg-checks OK over "
        f"{err_counts['cases']} cases"
    )
    print(
        f"rows: {row_counts['ok']}/{row_counts['checks']} leg-checks OK over "
        f"{row_counts['rows']} row cases ({row_counts['skipped']} leg-checks skipped)"
    )
    print(f"floor: {'OK' if not floor_failures else 'FAILED'}")
    return 1 if (floor_failures or vec_failures or row_failures or err_failures) else 0


if __name__ == "__main__":
    raise SystemExit(main())
