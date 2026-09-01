"""Python leg of the 3-way cross-binding parity harness (issue #1455).

Runs each fixture's SELECT through the PyO3 binding, canonicalizes every row
with ``canonical.py``, and writes ``out/py.<fixture>.json``.

Runnable standalone:

    python bindings/parity/driver.py --out-dir bindings/parity/out

Exit status: 0 on success; non-zero when the datasets are present but a query
raises or returns ZERO rows. A 0-row pass over a present corpus is the exact
false-green this repository forbids, so it is an ERROR here, never a skip.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from canonical import canon_row_python, types_from_columns  # noqa: E402

REPO_ROOT = _HERE.parents[1]
FIXTURES_PATH = _HERE / "fixtures.json"
DEFAULT_OUT_DIR = _HERE / "out"


class DriverError(Exception):
    """A leg failed to produce rows. Always fatal -- never downgraded to a skip."""


def resolve_datasets_root(explicit: Optional[str] = None) -> Path:
    """Mirror conftest.py's rule: accept either the parent of sstables/ or it."""
    raw = explicit or os.environ.get("CQLITE_DATASETS_ROOT")
    root = Path(raw) if raw else (REPO_ROOT / "test-data" / "datasets")
    candidate = root / "sstables"
    return candidate if candidate.exists() else root


def load_fixture_file(path: Path = FIXTURES_PATH) -> dict:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def load_fixtures(path: Path = FIXTURES_PATH) -> List[dict]:
    return load_fixture_file(path)["fixtures"]


def check_fixture_floor(data: Optional[dict] = None) -> List[str]:
    """The fixture set must not be able to shrink to nothing and stay green.

    Issue #1455 (B2), which is #3544's lesson applied to this table: an empty
    or truncated ``fixtures.json`` yields an EMPTY pytest parametrize -- one
    skipped placeholder and the 3-way comparison silently gone.
    """
    if data is None:
        data = load_fixture_file()
    floor = data.get("floor")
    if not isinstance(floor, dict):
        return ["fixtures.json has no `floor` block — the case floor cannot be checked"]
    fixtures = data.get("fixtures", [])
    names = [f["name"] for f in fixtures]
    failures: List[str] = []
    if len(fixtures) < floor["min_fixtures"]:
        failures.append(
            f"fixture floor: {len(fixtures)} < {floor['min_fixtures']} — fixtures were REMOVED"
        )
    missing = [n for n in floor["required_names"] if n not in names]
    if missing:
        failures.append(f"required fixture(s) absent: {missing}")
    if len(set(names)) != len(names):
        failures.append("duplicate fixture names")
    return failures


def fixture_types(fixture: dict) -> Dict[str, Any]:
    return types_from_columns(fixture["columns"])


def run_fixture(fixture: dict, datasets: Path) -> Dict[str, Any]:
    """Execute one fixture and return its canonical payload.

    Raises DriverError when the query yields no rows -- a present corpus that
    returns nothing is a failure, not an empty success.
    """
    import cqlite

    schema = REPO_ROOT / fixture["schema"]
    if not schema.exists():
        raise DriverError(f"schema not found: {schema}")
    types = fixture_types(fixture)
    with cqlite.open(str(datasets), schema=str(schema)) as db:
        result = db.execute(fixture["query"])
        raw_rows = list(result)
        # UNION over every row, never the last row's keys (issue #1455, B3): a
        # per-row assignment is last-row-wins, so a column missing from every
        # row BUT the last would pass while one missing only from the last row
        # would red. The CLI leg computes the same union, so the three legs are
        # compared under ONE rule.
        observed = sorted({key for row in raw_rows for key in row.keys()})
        rows = [canon_row_python(row, types) for row in raw_rows]
    if not rows:
        raise DriverError(
            f"fixture {fixture['name']!r} returned 0 rows from {datasets} "
            f"(query: {fixture['query']})"
        )
    return {
        "fixture": fixture["name"],
        "leg": "python",
        "query": fixture["query"],
        "observed_columns": observed,
        "rows": rows,
    }


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Python leg of the cross-binding parity harness")
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--datasets-root", default=None)
    parser.add_argument("--fixture", action="append", default=None, help="restrict to a fixture name")
    args = parser.parse_args(argv)

    datasets = resolve_datasets_root(args.datasets_root)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    data = load_fixture_file()
    floor_failures = check_fixture_floor(data)
    if floor_failures:
        for line in floor_failures:
            print(f"FAIL {line}", file=sys.stderr)
        return 2
    fixtures = data["fixtures"]
    if args.fixture:
        wanted = set(args.fixture)
        fixtures = [f for f in fixtures if f["name"] in wanted]
        missing = wanted - {f["name"] for f in fixtures}
        if missing:
            print(f"unknown fixture(s): {sorted(missing)}", file=sys.stderr)
            return 2
    if not fixtures:
        print("no fixtures selected", file=sys.stderr)
        return 2

    failures = 0
    for fixture in fixtures:
        try:
            payload = run_fixture(fixture, datasets)
        except Exception as exc:  # noqa: BLE001 - any failure is fatal for this leg
            print(f"FAIL {fixture['name']}: {type(exc).__name__}: {exc}", file=sys.stderr)
            failures += 1
            continue
        target = out_dir / f"py.{fixture['name']}.json"
        target.write_text(json.dumps(payload, indent=1, sort_keys=True), encoding="utf-8")
        print(f"OK   {fixture['name']}: {len(payload['rows'])} rows -> {target}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
