"""3-way golden parity: Python binding vs Node binding vs CLI (issue #1455).

The SAME ``SELECT`` is run through all three surfaces and canonicalized to ONE
JSON shape (``bindings/parity/canonical.py`` and its JS twin
``canonical.mjs``). Every leg must produce EQUAL canonical rows.

Why this exists: each binding was previously validated only against its OWN
oracle, so two bindings could drift apart while both stayed "green". This is
the CQLite-vs-CQLite differential for the SURFACE layer, the sibling of
``point_vs_full_differential.rs`` for the read path.

What it can NOT catch (declared, non-exhaustive -- see
``bindings/parity/README.md``):

* **tuple vs list.** Neither the Node binding nor the CLI distinguishes them
  (both emit a plain array), so the canonical form is a plain array on all
  three legs and a tuple/list confusion is INVISIBLE here.
* **A uniform error in all three legs.** All three read through
  ``cqlite-core``; agreement is agreement about CQLite, not about Cassandra.
  The sstabledump/JSONL oracles remain the authority for correctness.
* **``varint``.** No committed schema declares a varint column, so the varint
  canonicalization rule is pinned only by ``canonical-vectors.json``.

Marked ``slow`` deliberately: the CLI leg needs a RELEASE ``cqlite-cli`` build
and the Node leg needs a built native module. Nothing in the local agent gate
builds either, and the gate runs pytest with ``RUN_SLOW_TESTS=0`` -- leaving
this unmarked would silently add a full release build to every lane's gate.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

from conftest import (
    DATASETS,
    PROJECT_ROOT,
    _require_fixtures_strict,
    require_test_data,
)

PARITY_DIR = PROJECT_ROOT / "bindings" / "parity"
if str(PARITY_DIR) not in sys.path:
    sys.path.insert(0, str(PARITY_DIR))

from canonical import canon_row_cli  # noqa: E402
from driver import fixture_types, load_fixtures, run_fixture  # noqa: E402
from vectors import check_vectors as check_vectors_python  # noqa: E402

OUT_DIR = PARITY_DIR / "out"
NODE_DRIVER = PARITY_DIR / "driver.mjs"
NODE_VECTORS = PARITY_DIR / "vectors.mjs"
NODE_LIB = PROJECT_ROOT / "bindings" / "node" / "lib" / "index.js"

FIXTURES = load_fixtures()
FIXTURE_IDS = [f["name"] for f in FIXTURES]

# The Node leg needs an artifact NO local gate component builds. Requiring it
# whenever CQLITE_REQUIRE_FIXTURES=1 would red the existing python-ci `test`
# job, which sets that flag (it is about the DATASET corpus) and never
# provisions Node -- a false red on correct input. So the Node leg has its OWN
# fail-closed switch, set by the `cross-binding-parity` CI job.
REQUIRE_NODE_ENV = "CQLITE_PARITY_REQUIRE_NODE"


def _require_node_strict() -> bool:
    return os.environ.get(REQUIRE_NODE_ENV) in ("1", "true")


def _node_available() -> Optional[str]:
    """Reason the Node leg cannot run, or None when it can."""
    from shutil import which

    if which("node") is None:
        return "`node` is not on PATH"
    if not NODE_LIB.exists():
        return f"Node binding entry point not built: {NODE_LIB}"
    native = list((PROJECT_ROOT / "bindings" / "node").glob("*.node"))
    if not native:
        return "no built native module (bindings/node/*.node) — run `npm run build`"
    return None


# ---------------------------------------------------------------------------
# The comparator
# ---------------------------------------------------------------------------


def compare_legs(leg_rows: Dict[str, List[dict]]) -> List[str]:
    """Compare canonical rows across legs; return human-readable failures.

    Empty list == the legs agree. This is the single function the negative
    control below feeds deliberately-divergent input to, so that "the
    comparator can fail" is a tested property and not an assumption.

    A column ABSENT from one leg is compared as JSON ``null`` over the UNION of
    keys: the Python binding omits null columns while the CLI always emits
    them, so absence-vs-null must not be a difference -- but a genuinely wrong
    value still fails, because only the MISSING side is defaulted.
    """
    names = list(leg_rows)
    if len(names) < 2:
        return [f"need at least two legs to compare, got {names}"]

    failures: List[str] = []
    counts = {name: len(rows) for name, rows in leg_rows.items()}
    if len(set(counts.values())) != 1:
        failures.append(f"row COUNT differs across legs: {counts}")
        return failures
    if counts[names[0]] == 0:
        failures.append("every leg returned 0 rows (a present corpus must not be empty)")
        return failures

    for index in range(counts[names[0]]):
        rows = {name: leg_rows[name][index] for name in names}
        columns: List[str] = []
        for name in names:
            for key in rows[name]:
                if key not in columns:
                    columns.append(key)
        for column in sorted(columns):
            values = {name: rows[name].get(column) for name in names}
            first = values[names[0]]
            if all(_same(first, values[name]) for name in names[1:]):
                continue
            rendered = "\n".join(
                f"    {name:<7} = {json.dumps(values[name], ensure_ascii=False)}"
                f"   ({_shape(values[name])})"
                for name in names
            )
            failures.append(
                f"row {index}, column {column!r} differs across legs:\n{rendered}"
            )
            # FIRST difference only: a whole-row dump of a wide table buries
            # the signal, and every later column is usually the same defect.
            return failures
    return failures


def _same(a: Any, b: Any) -> bool:
    return a == b and _shape(a) == _shape(b)


def _shape(v: Any) -> str:
    """Type-tagged shape, so ``1`` never equals ``"1"`` or ``True``."""
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
        return "[" + ",".join(_shape(x) for x in v) + "]"
    if isinstance(v, dict):
        return "{" + ",".join(f"{k}:{_shape(v[k])}" for k in sorted(v)) + "}"
    return type(v).__name__


# ---------------------------------------------------------------------------
# Leg runners
# ---------------------------------------------------------------------------


def _fail(message: str) -> None:
    pytest.fail(message, pytrace=False)


def run_node_leg(fixture: dict, out_dir: Path) -> dict:
    """Run bindings/parity/driver.mjs for ONE fixture and read its artifact."""
    cmd = [
        "node",
        str(NODE_DRIVER),
        "--fixture",
        fixture["name"],
        "--out-dir",
        str(out_dir),
        "--datasets-root",
        str(DATASETS),
    ]
    proc = subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=True, text=True, timeout=300)
    if proc.returncode != 0:
        raise RuntimeError(
            f"node driver exited {proc.returncode}\n"
            f"stdout: {proc.stdout.strip()}\nstderr: {proc.stderr.strip()}"
        )
    artifact = out_dir / f"node.{fixture['name']}.json"
    if not artifact.exists():
        raise RuntimeError(f"node driver produced no artifact at {artifact}")
    with artifact.open(encoding="utf-8") as handle:
        return json.load(handle)


def run_cli_leg(fixture: dict, cli_binary: Path, out_dir: Path) -> dict:
    """Run the CLI with --out json and canonicalize its rows."""
    cmd = [
        str(cli_binary),
        "--data-dir",
        str(DATASETS),
        "--schema",
        str(PROJECT_ROOT / fixture["schema"]),
        "--query",
        fixture["query"],
        "--out",
        "json",
        # The CLI caps at 1000 rows by default; the fixtures carry their own
        # LIMIT, so this only removes the implicit cap.
        "--limit",
        "100000",
    ]
    proc = subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=True, text=True, timeout=300)
    if proc.returncode != 0:
        raise RuntimeError(
            f"cqlite CLI exited {proc.returncode}\nstderr: {proc.stderr.strip()[-2000:]}"
        )
    raw_rows = json.loads(proc.stdout)
    if not isinstance(raw_rows, list):
        raise RuntimeError(f"CLI --out json did not produce a top-level array: {type(raw_rows)}")
    if not raw_rows:
        raise RuntimeError(
            f"fixture {fixture['name']!r} returned 0 rows from the CLI (query: {fixture['query']})"
        )
    types = fixture_types(fixture)
    observed = sorted({key for row in raw_rows for key in row})
    payload = {
        "fixture": fixture["name"],
        "leg": "cli",
        "query": fixture["query"],
        "observed_columns": observed,
        "rows": [canon_row_cli(row, types) for row in raw_rows],
    }
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / f"cli.{fixture['name']}.json").write_text(
        json.dumps(payload, indent=1, sort_keys=True), encoding="utf-8"
    )
    return payload


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_canonicalizer_vectors_python_leg():
    """The Python canonicalizer matches canonical-vectors.json exactly.

    Not slow and dataset-independent: it is pure computation over a committed
    table, and it is what makes the Python/JS twins KNOWN to agree rather than
    assumed to.
    """
    failures = check_vectors_python()
    assert not failures, "canonical vector mismatches (python/cli legs):\n" + "\n".join(failures)


def test_fixtures_declare_no_divergence_allowlist():
    """`known_divergence` must stay EMPTY.

    The allowlist key exists so a future, deliberately-accepted gap has a home
    with a reason attached. Populating it to make this suite green would hide
    exactly the cross-binding drift the harness exists to find, so the harness
    refuses the shortcut on its own behalf.
    """
    populated = {f["name"]: f["known_divergence"] for f in FIXTURES if f["known_divergence"]}
    assert not populated, (
        "known_divergence is populated: " + json.dumps(populated) + ". A real divergence is a "
        "BUG to report, not an entry to add — see bindings/parity/README.md."
    )


@pytest.mark.slow
def test_canonicalizer_vectors_node_leg():
    """The JS canonicalizer matches the SAME canonical-vectors.json.

    Only ``node`` is needed (``vectors.mjs`` does not load the native module),
    but it is marked slow so it lands in the same CI lane as the rest of the
    harness rather than in a job that never provisions Node.
    """
    from shutil import which

    if which("node") is None:
        if _require_node_strict():
            _fail(f"`node` is not on PATH but {REQUIRE_NODE_ENV}=1")
        pytest.skip("`node` is not on PATH")
    proc = subprocess.run(
        ["node", str(NODE_VECTORS)], cwd=PROJECT_ROOT, capture_output=True, text=True, timeout=120
    )
    assert proc.returncode == 0, (
        "JS canonicalizer disagrees with canonical-vectors.json:\n"
        f"{proc.stdout}\n{proc.stderr}"
    )


@pytest.mark.slow
@pytest.mark.parametrize("fixture", FIXTURES, ids=FIXTURE_IDS)
def test_three_way_parity(fixture, cli_binary, capsys):
    """Python binding == Node binding == CLI, as canonical JSON, per row."""
    require_test_data(PROJECT_ROOT / fixture["schema"])

    out_dir = OUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    legs: Dict[str, List[dict]] = {}
    declared = sorted(fixture["columns"])
    notes: List[str] = []

    # --- Python leg (in-process) --------------------------------------------
    try:
        py_payload = run_fixture(fixture, DATASETS)
    except Exception as exc:  # noqa: BLE001 - a present corpus must not fail
        _fail(f"python leg failed for {fixture['name']!r}: {type(exc).__name__}: {exc}")
    (out_dir / f"py.{fixture['name']}.json").write_text(
        json.dumps(py_payload, indent=1, sort_keys=True), encoding="utf-8"
    )
    legs["python"] = py_payload["rows"]

    # --- CLI leg -------------------------------------------------------------
    try:
        cli_payload = run_cli_leg(fixture, cli_binary, out_dir)
    except Exception as exc:  # noqa: BLE001
        _fail(f"cli leg failed for {fixture['name']!r}: {type(exc).__name__}: {exc}")
    legs["cli"] = cli_payload["rows"]

    # --- Node leg ------------------------------------------------------------
    reason = _node_available()
    if reason is None:
        try:
            node_payload = run_node_leg(fixture, out_dir)
        except Exception as exc:  # noqa: BLE001
            _fail(f"node leg failed for {fixture['name']!r}: {type(exc).__name__}: {exc}")
        legs["node"] = node_payload["rows"]
        _assert_columns("node", node_payload, declared)
    elif _require_node_strict():
        _fail(f"node leg unavailable ({reason}) but {REQUIRE_NODE_ENV}=1")
    else:
        notes.append(
            f"DECLARED OMISSION: the NODE leg did not run ({reason}). "
            "This run compared python vs cli ONLY — 2 of 3 legs. Set "
            f"{REQUIRE_NODE_ENV}=1 to make this a failure."
        )

    _assert_columns("python", py_payload, declared)
    _assert_columns("cli", cli_payload, declared)

    # Every run STATES which legs it compared: a lane that omits coverage
    # silently is indistinguishable from one that covers it.
    header = (
        f"[cross-binding parity] fixture={fixture['name']} "
        f"legs_compared={sorted(legs)} rows={len(legs['python'])} "
        f"columns={len(declared)}"
    )
    gaps = (
        "[cross-binding parity] DECLARED GAPS (non-exhaustive): tuple-vs-list is "
        "indistinguishable on the node and cli legs; no varint column exists in the "
        "committed corpus; a uniform cqlite-core defect is invisible to a "
        "CQLite-vs-CQLite differential."
    )
    with capsys.disabled():
        print("\n" + header)
        for note in notes:
            print(f"[cross-binding parity] {note}")
        print(gaps)

    failures = compare_legs(legs)
    assert not failures, (
        f"{header}\n" + "\n".join(notes) + ("\n" if notes else "") + "\n".join(failures)
    )


def _assert_columns(leg: str, payload: dict, declared: List[str]) -> None:
    observed = sorted(payload["observed_columns"])
    assert observed == declared, (
        f"[{leg}] column set drifted from fixtures.json for {payload['fixture']!r}: "
        f"declared={declared} observed={observed}. Update fixtures.json's `columns` "
        "(it is the authoritative type source for canonicalization)."
    )


# ---------------------------------------------------------------------------
# Negative control -- proves the comparator CAN fail
# ---------------------------------------------------------------------------


def test_comparator_detects_value_divergence():
    a = [{"id": "aaaa", "n": 1}]
    b = [{"id": "aaaa", "n": 2}]
    failures = compare_legs({"python": a, "node": b, "cli": a})
    assert failures, "comparator accepted two legs with a different value"
    assert "column 'n'" in failures[0]


def test_comparator_detects_type_divergence_without_value_divergence():
    """``1`` and ``"1"`` must NOT compare equal.

    This is the integer rule's whole point: a leg that emitted a big integer as
    a JSON number while another emitted a decimal string is a real divergence,
    and a plain ``==`` in Python would not always catch the bool/int case.
    """
    failures = compare_legs({"python": [{"n": 1}], "node": [{"n": "1"}]})
    assert failures, "comparator accepted an int on one leg and a string on another"
    failures = compare_legs({"python": [{"b": True}], "node": [{"b": 1}]})
    assert failures, "comparator accepted True on one leg and 1 on another"


def test_comparator_detects_row_count_divergence():
    failures = compare_legs({"python": [{"n": 1}], "node": [{"n": 1}, {"n": 2}]})
    assert failures and "row COUNT differs" in failures[0]


def test_comparator_detects_row_order_divergence():
    """Row ORDER is compared, never sorted away.

    Measured on this corpus: all three legs return rows in the SAME order for
    every fixture, so an order difference is a real finding and the comparator
    must surface it rather than normalize it.
    """
    a = [{"id": "x"}, {"id": "y"}]
    failures = compare_legs({"python": a, "node": list(reversed(a))})
    assert failures and "row 0" in failures[0]


def test_comparator_treats_absent_column_as_null_not_a_difference():
    """The Python binding omits null columns; the CLI always emits them."""
    failures = compare_legs({"python": [{"id": "x"}], "cli": [{"id": "x", "extra": None}]})
    assert not failures, failures


def test_comparator_still_fails_when_the_present_side_is_not_null():
    failures = compare_legs({"python": [{"id": "x"}], "cli": [{"id": "x", "extra": 5}]})
    assert failures and "column 'extra'" in failures[0]


def test_comparator_rejects_zero_rows_on_every_leg():
    failures = compare_legs({"python": [], "node": [], "cli": []})
    assert failures and "0 rows" in failures[0]


def test_comparator_needs_at_least_two_legs():
    assert compare_legs({"python": [{"n": 1}]})


def test_strict_mode_helpers_are_wired():
    """The dataset guard this module relies on is the shared, strict-aware one."""
    assert callable(_require_fixtures_strict)
    assert callable(require_test_data)
