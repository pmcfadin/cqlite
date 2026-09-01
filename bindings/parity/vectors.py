"""Differential pin for the Python canonicalizer (issue #1455).

``canonical.py`` and ``canonical.mjs`` are two independent implementations of
one written spec. This module materializes every vector in
``canonical-vectors.json`` into the PYTHON binding's native value shape (and
the CLI's parsed-JSON shape) and checks both against the vector's expected
canonical output. ``vectors.mjs`` does the same for the Node shape.

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
from typing import Any, List, Tuple

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from canonical import canon_cli, canon_python, parse_type  # noqa: E402

VECTORS_PATH = _HERE / "canonical-vectors.json"
_EPOCH = _dt.datetime(1970, 1, 1, tzinfo=_dt.timezone.utc)


def load_vectors(path: Path = VECTORS_PATH) -> List[dict]:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)["vectors"]


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


def check_vectors(vectors: List[dict] | None = None) -> List[str]:
    """Return a list of human-readable failures; empty means both legs agree."""
    failures: List[str] = []
    for vec in vectors if vectors is not None else load_vectors():
        t = parse_type(vec["type"])
        expected = vec["canonical"]
        for leg, canon in (("python", canon_python), ("cli", canon_cli)):
            if leg == "cli" and vec.get("cli") is None and expected is not None:
                # An explicit "unreachable from the CLI" vector; python-only.
                continue
            try:
                value = materialize_python(vec[leg]) if leg == "python" else vec["cli"]
                actual = canon(value, t)
            except Exception as exc:  # noqa: BLE001 - report, do not abort the sweep
                failures.append(f"{vec['name']}/{leg}: raised {type(exc).__name__}: {exc}")
                continue
            if actual != expected or _typed(actual) != _typed(expected):
                failures.append(
                    f"{vec['name']}/{leg}: expected {expected!r} ({_typed(expected)}), "
                    f"got {actual!r} ({_typed(actual)})"
                )
    return failures


def _typed(v: Any) -> str:
    """Type-tagged shape, so ``1`` and ``\"1\"`` (or ``1`` and ``True``) never
    compare equal by accident -- the integer rule's whole point is WHICH JSON
    type a value lands in."""
    if isinstance(v, bool):
        return "bool"
    if isinstance(v, int):
        return "int"
    if isinstance(v, float):
        return "float"
    if isinstance(v, str):
        return "str"
    if v is None:
        return "null"
    if isinstance(v, list):
        return "[" + ",".join(_typed(x) for x in v) + "]"
    if isinstance(v, dict):
        return "{" + ",".join(f"{k}:{_typed(v[k])}" for k in sorted(v)) + "}"
    return type(v).__name__


def main() -> int:
    vectors = load_vectors()
    failures = check_vectors(vectors)
    for line in failures:
        print(f"FAIL {line}", file=sys.stderr)
    print(f"{len(vectors) - len(failures)}/{len(vectors)} vectors OK (python + cli legs)")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
