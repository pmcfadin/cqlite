"""3-way golden parity: Python binding vs Node binding vs CLI (issue #1455).

The SAME ``SELECT`` is run through all three surfaces and canonicalized to ONE
JSON shape (``bindings/parity/canonical.py`` and its JS twin
``canonical.mjs``). Every leg must produce EQUAL canonical rows.

Why this exists: each binding was previously validated only against its OWN
oracle, so two bindings could drift apart while both stayed "green". This is
the CQLite-vs-CQLite differential for the SURFACE layer, the sibling of
``point_vs_full_differential.rs`` for the read path.

The declared gaps are listed in ``DECLARED_GAPS`` below, printed IN FULL by
every ``test_three_way_parity`` run, and explained in
``bindings/parity/README.md``.

Marked ``slow`` deliberately: the CLI leg needs a RELEASE ``cqlite-cli`` build
and the Node leg needs a built native module. Nothing in the local agent gate
builds either, and the gate runs pytest with ``RUN_SLOW_TESTS=0`` -- leaving
this unmarked would silently add a full release build to every lane's gate.
The non-slow half (the canonicalizer pin, the case floors and the schema
census) is dependency-free and DOES run in the gate.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytest

from conftest import (
    DATASETS,
    PROJECT_ROOT,
    _require_fixtures_strict,
    skip_if_no_datasets,
)

PARITY_DIR = PROJECT_ROOT / "bindings" / "parity"
if str(PARITY_DIR) not in sys.path:
    sys.path.insert(0, str(PARITY_DIR))

from canonical import (  # noqa: E402
    CanonicalError,
    CqlType,
    canon_python,
    canon_row_cli,
    canonical_equal,
    parse_type,
    shape_tag,
    subtree_has_udt,
)
from driver import (  # noqa: E402
    check_fixture_floor,
    fixture_types,
    load_fixture_file,
    run_fixture,
)
from vectors import (  # noqa: E402
    ALL_LEGS,
    check_errors,
    check_floor,
    check_rows,
    check_schema,
    check_vectors,
    load_all as load_vector_file,
)

OUT_DIR = PARITY_DIR / "out"
NODE_DRIVER = PARITY_DIR / "driver.mjs"
NODE_VECTORS = PARITY_DIR / "vectors.mjs"
NODE_LIB = PROJECT_ROOT / "bindings" / "node" / "lib" / "index.js"

FIXTURE_FILE = load_fixture_file()
FIXTURES = FIXTURE_FILE["fixtures"]
FIXTURE_IDS = [f["name"] for f in FIXTURES]

# The Node leg needs an artifact NO local gate component builds. Requiring it
# whenever CQLITE_REQUIRE_FIXTURES=1 would red the existing python-ci `test`
# job, which sets that flag (it is about the DATASET corpus) and never
# provisions Node -- a false red on correct input. So the Node leg has its OWN
# fail-closed switch, set by the `cross-binding-parity` CI job.
REQUIRE_NODE_ENV = "CQLITE_PARITY_REQUIRE_NODE"

# Printed IN FULL by every 3-way run (issue #1455, N2). An earlier version
# printed 3 of these 7 under a claim that it printed them all -- a false
# rationale in a test log is worse than none, because it is what stops the next
# person looking. Full text: bindings/parity/README.md.
DECLARED_GAPS: Tuple[str, ...] = (
    "tuple vs list is UNDETECTABLE here: the node and cli legs both emit a plain array.",
    "no `varint` column exists in the committed schema corpus; the rule is pinned by "
    "canonical-vectors.json alone.",
    "UDT columns are REFUSED by the canonicalizer, not compared; no fixture uses one.",
    "non-finite floats are a real 3-way asymmetry (python nan / node NaN / cli null) and "
    "are avoided rather than reconciled.",
    "a column absent from one leg is compared as null, so this harness cannot tell "
    "'omitted' from 'null' (the NODE leg is the one that omits — bindings/node/src/row.rs).",
    "a UNIFORM cqlite-core defect is invisible: all three legs read the same core, so "
    "agreement here is agreement about CQLite, not about Cassandra.",
    "the 3-way comparison runs in CI only (python-ci.yml / cross-binding-parity); no local "
    "agent-gate component builds a release CLI or the node native module.",
)


# ---------------------------------------------------------------------------
# Strict-aware leg availability
# ---------------------------------------------------------------------------


def _require_node_strict() -> bool:
    return os.environ.get(REQUIRE_NODE_ENV) in ("1", "true")


def _node_available() -> Optional[str]:
    """Reason the Node leg cannot run, or None when it can."""
    if shutil.which("node") is None:
        return "`node` is not on PATH"
    if not NODE_LIB.exists():
        return f"Node binding entry point not built: {NODE_LIB}"
    if not list((PROJECT_ROOT / "bindings" / "node").glob("*.node")):
        return "no built native module (bindings/node/*.node) — run `npm run build`"
    return None


def node_leg_disposition(reason: Optional[str], strict: bool) -> Tuple[str, Optional[str]]:
    """Decide what an unavailable Node leg means. Extracted so it is TESTABLE.

    Returns ``("run", None)`` / ``("fail", message)`` / ``("omit", note)``. The
    call site in ``test_three_way_parity`` is a three-line dispatch over this,
    so the fail-closed switch that keeps the CI job honest is covered by unit
    tests rather than by nothing (issue #1455, B5).
    """
    if reason is None:
        return "run", None
    if strict:
        return "fail", f"node leg unavailable ({reason}) but {REQUIRE_NODE_ENV}=1"
    return "omit", (
        f"DECLARED OMISSION: the NODE leg did not run ({reason}). "
        f"This run compared python vs cli ONLY — 2 of 3 legs. Set "
        f"{REQUIRE_NODE_ENV}=1 to make this a failure."
    )


def _fail(message: str) -> None:
    pytest.fail(message, pytrace=False)


@pytest.fixture(scope="session")
def parity_out_dir() -> Path:
    """A CLEAN artifact directory, once per session (issue #1455, N9).

    A stale ``out/node.<fixture>.json`` from an earlier run would otherwise be
    readable as this run's output if the driver died after printing but before
    writing — a green comparison of last week's data.
    """
    if OUT_DIR.exists():
        shutil.rmtree(OUT_DIR)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    return OUT_DIR


def resolve_parity_cli_binary(get_cli_binary) -> Path:
    """Strict-aware wrapper around conftest's ``cli_binary`` (issue #1455, B1).

    ``conftest.cli_binary`` ``pytest.skip``s on a build failure, a build
    timeout, absent cargo, or a missing binary, and is NOT strict-aware. In the
    CI job the invocation is THIS FILE ONLY, whose non-slow tests pass, so
    #1230's "no tests ran" session floor never fires: all three parity cases
    would skip and ``cross-binding-parity`` would report SUCCESS having
    compared nothing. Under either strict switch that skip becomes a FAILURE.

    ``conftest.py`` is deliberately NOT edited -- other suites depend on the
    lenient skip. Taking the accessor as an argument (rather than reading
    ``request`` here) is what makes BOTH branches unit-testable without a
    cargo build; the fixture below is a one-line adapter.
    """
    strict = _require_fixtures_strict() or _require_node_strict()
    try:
        binary = get_cli_binary()
    except pytest.skip.Exception as exc:
        message = f"CLI binary unavailable for the 3-way parity harness: {exc}"
        if strict:
            _fail(
                message
                + " (strict mode: CQLITE_REQUIRE_FIXTURES / "
                + REQUIRE_NODE_ENV
                + " is set, so a skipped CLI leg would be a green run that compared nothing)"
            )
        pytest.skip(message)
    if not Path(binary).exists():
        message = f"CLI binary path does not exist: {binary}"
        if strict:
            _fail(message + " (strict mode)")
        pytest.skip(message)
    return Path(binary)


@pytest.fixture(scope="session")
def parity_cli_binary(request) -> Path:
    return resolve_parity_cli_binary(lambda: request.getfixturevalue("cli_binary"))


# ---------------------------------------------------------------------------
# The comparator
# ---------------------------------------------------------------------------


def compare_legs(leg_rows: Dict[str, List[dict]]) -> List[str]:
    """Compare canonical rows across legs; return human-readable failures.

    Empty list == the legs agree. This is the single function the negative
    controls below feed deliberately-divergent input to, so that "the
    comparator can fail" is a tested property and not an assumption.

    A column ABSENT from one leg is compared as JSON ``null`` over the UNION of
    keys. Measured, the leg that omits is the NODE one
    (``bindings/node/src/row.rs:123-138`` skips a metadata column with no
    value); the Python binding null-FILLS a shared row shape
    (``bindings/python/src/result.rs:184-192,447``) and the CLI always emits
    every column. Absence must therefore not be a difference -- but a genuinely
    wrong value still fails, because only the MISSING side is defaulted.
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
            if all(canonical_equal(first, values[name]) for name in names[1:]):
                continue
            rendered = "\n".join(
                f"    {name:<7} = {json.dumps(values[name], ensure_ascii=False)}"
                f"   ({shape_tag(values[name])})"
                for name in names
            )
            failures.append(
                f"row {index}, column {column!r} differs across legs:\n{rendered}"
            )
            # FIRST difference only: a whole-row dump of a wide table buries
            # the signal, and every later column is usually the same defect.
            return failures
    return failures


# ---------------------------------------------------------------------------
# Leg runners
# ---------------------------------------------------------------------------


def run_node_leg(fixture: dict, out_dir: Path) -> dict:
    """Run bindings/parity/driver.mjs for ONE fixture and read its artifact."""
    artifact = out_dir / f"node.{fixture['name']}.json"
    if artifact.exists():
        artifact.unlink()
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
    if not artifact.exists():
        raise RuntimeError(f"node driver produced no artifact at {artifact}")
    with artifact.open(encoding="utf-8") as handle:
        payload = json.load(handle)
    # A stale artifact must never be read as fresh (issue #1455, N9). The file
    # is deleted above, so this is the second half of the same guard: the
    # artifact must say it is about THIS fixture and THIS query.
    if payload.get("fixture") != fixture["name"] or payload.get("query") != fixture["query"]:
        raise RuntimeError(
            f"node artifact identity mismatch at {artifact}: "
            f"fixture={payload.get('fixture')!r} query={payload.get('query')!r}"
        )
    return payload


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
    # UNION over every row — the SAME rule both drivers use (issue #1455, B3).
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


def assert_leg_columns(leg: str, payload: dict, declared: List[str]) -> None:
    """A leg may OMIT a declared column; it may never invent one.

    Consistent with ``compare_legs``' absence-is-null rule (issue #1455, B3):
    demanding the full declared set from EVERY leg would contradict it, since
    the Node leg legitimately omits a column with no value. What is asserted
    per leg is therefore a SUBSET relation; that the union across legs covers
    the declared set is asserted once, in ``assert_union_columns``.
    """
    observed = sorted(payload["observed_columns"])
    undeclared = [c for c in observed if c not in declared]
    assert not undeclared, (
        f"[{leg}] returned column(s) not declared in fixtures.json for "
        f"{payload['fixture']!r}: {undeclared}. fixtures.json's `columns` is the "
        "authoritative type source for canonicalization — add them there."
    )


def assert_union_columns(payloads: Dict[str, dict], declared: List[str]) -> None:
    union = sorted({c for p in payloads.values() for c in p["observed_columns"]})
    missing = [c for c in declared if c not in union]
    assert not missing, (
        f"column(s) declared in fixtures.json but returned by NO leg: {missing} "
        f"(legs: {sorted(payloads)}). Either the schema changed or the query did."
    )


# ---------------------------------------------------------------------------
# Non-slow: canonicalizer pin, case floors, committed-source census
# ---------------------------------------------------------------------------


def test_canonicalizer_vectors_python_leg():
    """The Python canonicalizer matches canonical-vectors.json exactly.

    Not slow and dataset-independent: pure computation over a committed table,
    and it is what makes the Python/JS twins KNOWN to agree rather than assumed
    to. Covers both the value vectors and the refusal cases.
    """
    failures, counts = check_vectors()
    assert not failures, "canonical vector mismatches (python/cli legs):\n" + "\n".join(failures)
    assert counts["checks"] > 0, "no vector leg-checks ran"
    assert counts["ok"] == counts["checks"]


def test_canonicalizer_refusals_python_leg():
    """Malformed input must RAISE, naming the reason — never be guessed at."""
    failures, counts = check_errors()
    assert not failures, "refusal-case failures (python/cli legs):\n" + "\n".join(failures)
    assert counts["checks"] > 0, "no refusal leg-checks ran"


def test_canonicalizer_row_cases_python_leg():
    """The ROW-BUILDING path, including hostile column NAMES (issue #1455, F1).

    ``__proto__`` is a legal quoted CQL identifier (this repo already ships
    ``test-data/schemas/issue-3630-row-collision.cql`` for it) and on an
    ordinary JS object assigning it replaces the prototype instead of creating
    an own property -- silently, so the Node leg would emit a row missing that
    column and this harness would report agreement about data it had dropped.
    Python dicts are immune; both legs are driven against the SAME expected row
    so the immune half pins the vulnerable one.

    NOTE: no ``test_row_collision`` SSTable exists in either corpus root, so
    there is no live 3-way FIXTURE for this — these row cases plus the Node
    runner's own ``checkRows`` are the coverage, and the README says so.
    """
    failures, counts = check_rows()
    assert not failures, "row-case failures (python/cli legs):\n" + "\n".join(failures)
    assert counts["checks"] > 0, "no row leg-checks ran"
    assert counts["ok"] == counts["checks"]


def test_canonical_vector_file_schema():
    """Every case must CARRY every field the runners read (issue #1455, F3).

    The class: a ``.get(key, default)`` / ``|| []`` read lets an ABSENT field
    take the permissive branch, so an accidentally deleted leg key silently
    skips that leg and the differential pin stays green over a shrunken subject
    set. The floors count CASES; this requires each case to be COMPLETE.
    """
    assert not check_schema(), "\n".join(check_schema())


@pytest.mark.parametrize("section", ["vectors", "rows"])
@pytest.mark.parametrize("leg", ALL_LEGS)
def test_every_case_carries_every_leg(section, leg):
    """Stated as a test as well as a runner check, so it cannot be lost with it."""
    data = load_vector_file()
    missing = [c["name"] for c in data[section] if leg not in c]
    assert not missing, f"{section} case(s) missing the {leg!r} leg: {missing}"


def test_container_kinds_are_type_specific_not_interchangeable():
    """A list/set/tuple swap must RED, not be normalized away (issue #1455, F4).

    Accepting the three interchangeably was a hole in the harness's core
    purpose: a binding regression returning an ``Array`` for ``set<text>`` (or
    a ``Set`` for ``list<int>``) is a change to a public API shape and is
    exactly the cross-binding drift this exists to catch. Only the python and
    node legs can enforce it — the CLI renders all three as a bare JSON array
    (README gap 1).
    """
    with pytest.raises(CanonicalError, match=r"declared set<> expects a Python frozenset/set"):
        canon_python(["a"], parse_type("set<text>"))
    with pytest.raises(CanonicalError, match=r"declared list<> expects a Python list"):
        canon_python(("a",), parse_type("list<text>"))
    with pytest.raises(CanonicalError, match=r"declared tuple<> expects a Python tuple"):
        canon_python(["a"], parse_type("tuple<text>"))
    with pytest.raises(CanonicalError, match=r"declared map<> expects a Python dict"):
        canon_python([], parse_type("map<text, text>"))
    # ...and the correct shapes still pass.
    assert canon_python(frozenset({"b", "a"}), parse_type("set<text>")) == ["a", "b"]
    assert canon_python(["b", "a"], parse_type("list<text>")) == ["b", "a"]
    assert canon_python(("b", "a"), parse_type("tuple<text, text>")) == ["b", "a"]


def test_hashable_position_projection_is_allowed_not_red():
    """INTENTIONAL projection #2 — `value_hashable.rs`, measured, not guessed.

    Inside a ``set`` element or a ``map`` KEY the Python binding projects every
    container: ``list``/``tuple`` -> ``tuple``, ``map`` -> ``tuple`` of
    2-``tuple``s. A context-free strict check would red on correct input here,
    which is the guard agents learn to waive.
    """
    assert canon_python(
        frozenset({(1, 9), (2,)}), parse_type("set<frozen<list<int>>>")
    ) == [[1, 9], [2]]
    assert canon_python(
        {(("a", 1),): "x"}, parse_type("map<frozen<map<text, int>>, text>")
    ) == [[[["a", 1]], "x"]]
    # A map VALUE is NOT a hashable position, so a real list is correct there.
    assert canon_python({"a": [2, 1]}, parse_type("map<text, frozen<list<int>>>")) == [
        ["a", [2, 1]]
    ]


def test_set_of_udt_projection_is_allowed():
    """INTENTIONAL projection #1 — SET<FROZEN<UDT>> arrives as a `list` (#804/#3500).

    Measured at source: ``bindings/python/src/value.rs::set_to_py`` branches on
    ``items.iter().any(contains_udt)`` — UDT-CONTAINMENT, not unhashability.

    The type tree is built DIRECTLY here because ``parse_type`` refuses a UDT
    name, so the allowance is currently unreachable through the public entry
    point. Keeping the branch live and tested is the point: adding UDT support
    later must not silently turn a correct binding red.
    """
    from canonical import PYTHON_ADAPTER

    udt_set = CqlType("set", (CqlType("udt", ()),))
    assert subtree_has_udt(udt_set)
    assert not subtree_has_udt(parse_type("set<frozen<list<int>>>"))
    # The #804 `list` projection is ACCEPTED for a UDT subtree. Asserted at
    # `as_seq` rather than through `canon_python`, because canonicalizing the
    # UDT SCALAR is a separate declared gap (UDTs are refused).
    assert PYTHON_ADAPTER.as_seq([], udt_set, False) == []
    # ...and it is refused inside a HASHABLE position, where value_hashable.rs
    # never re-enters set_to_py, so the UDT branch is unreachable there.
    with pytest.raises(CanonicalError, match=r"declared set<> expects a Python frozenset/set"):
        PYTHON_ADAPTER.as_seq([], udt_set, True)
    # ...and refused for a subtree WITHOUT a UDT, which is F4's whole point.
    with pytest.raises(CanonicalError, match=r"declared set<> expects a Python frozenset/set"):
        canon_python([[1]], parse_type("set<frozen<list<int>>>"))


def test_canonical_vector_case_floor():
    """The vector table cannot shrink to nothing and stay green (B2).

    #3544's lesson applied to this harness: ``check_vectors([])`` returns no
    failures and ``vectors.mjs`` would print ``0/0 vectors OK`` and exit 0.
    """
    data = load_vector_file()
    assert not check_floor(data), "\n".join(check_floor(data))
    assert len(data["vectors"]) >= data["floor"]["min_vectors"] >= 59
    assert len(data["errors"]) >= data["floor"]["min_errors"] >= 20
    assert len(data["rows"]) >= data["floor"]["min_rows"] >= 4
    # The hostile-column-name cases are the reason `rows` exists; a floor on the
    # COUNT alone would let them be swapped for four benign rows.
    assert {"proto_polluting_column_names", "proto_column_absent_from_the_row"} <= {
        r["name"] for r in data["rows"]
    }


def test_fixture_case_floor():
    """The fixture set cannot shrink to nothing and stay green (B2).

    An empty ``fixtures.json`` yields an EMPTY parametrize — one skipped
    placeholder and the 3-way comparison silently gone.
    """
    failures = check_fixture_floor(FIXTURE_FILE)
    assert not failures, "\n".join(failures)
    assert len(FIXTURES) >= 3
    assert set(FIXTURE_IDS) >= {"basic_types", "collections", "typed_collections"}


def test_fixture_schemas_are_committed_and_present():
    """Every fixture's schema is COMMITTED SOURCE and must exist — no skip (B1).

    ``conftest.skip_if_no_schema`` (reached via ``require_test_data``) is a
    plain ``pytest.skip``, so a typo'd or renamed ``schema`` in fixtures.json
    would silently drop a fixture even under ``CQLITE_REQUIRE_FIXTURES=1``. Per
    #3148, committed source in a checkout is never legitimately absent, so this
    assertion is UNCONDITIONAL.
    """
    missing = [
        (f["name"], f["schema"])
        for f in FIXTURES
        if not (PROJECT_ROOT / f["schema"]).is_file()
    ]
    assert not missing, f"fixture schema file(s) absent from the checkout: {missing}"


def test_fixtures_declare_no_divergence_allowlist():
    """`known_divergence` must stay EMPTY, and every fixture must declare it.

    The allowlist key exists so a future, deliberately-accepted gap has a home
    with a reason attached. Populating it to make this suite green would hide
    exactly the cross-binding drift the harness exists to find.
    """
    assert FIXTURES, "no fixtures — see test_fixture_case_floor"
    undeclared = [f["name"] for f in FIXTURES if "known_divergence" not in f]
    assert not undeclared, f"fixture(s) with no `known_divergence` key: {undeclared}"
    populated = {f["name"]: f["known_divergence"] for f in FIXTURES if f["known_divergence"]}
    assert not populated, (
        "known_divergence is populated: " + json.dumps(populated) + ". A real divergence is a "
        "BUG to report, not an entry to add — see bindings/parity/README.md."
    )


def test_declared_gaps_are_stated_in_full():
    """The runtime declaration must cover every gap the README's list has (N2).

    Scoped to the ``## DECLARED GAPS`` SECTION, not the whole file: counting
    numbered items anywhere would silently absorb an unrelated numbered list
    added later, which is the same "claim wider than the measurement" defect
    this test exists to prevent.
    """
    readme = (PARITY_DIR / "README.md").read_text(encoding="utf-8")
    marker = "\n## DECLARED GAPS\n"
    assert marker in readme, "README lost its `## DECLARED GAPS` section"
    section = readme.split(marker, 1)[1].split("\n## ", 1)[0]
    documented = [
        line for line in section.split("\n") if re.match(r"^\d+\. \*\*", line)
    ]
    assert len(documented) == len(DECLARED_GAPS), (
        f"README's DECLARED GAPS section lists {len(documented)} gaps but "
        f"DECLARED_GAPS has {len(DECLARED_GAPS)} — the claim that every run states "
        "them all must stay true"
    )


# ---------------------------------------------------------------------------
# Slow: the three legs
# ---------------------------------------------------------------------------


def _skip_or_fail_without_node():
    reason = _node_available()
    if reason is None:
        return
    if _require_node_strict():
        _fail(f"{reason} but {REQUIRE_NODE_ENV}=1")
    pytest.skip(reason)


@pytest.mark.slow
def test_canonicalizer_vectors_node_leg():
    """The JS canonicalizer matches the SAME canonical-vectors.json.

    Only ``node`` is needed (``vectors.mjs`` does not load the native module),
    but it is marked slow so it lands in the same CI lane as the rest of the
    harness rather than in a job that never provisions Node.
    """
    if shutil.which("node") is None:
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
def test_node_canonical_survives_the_json_round_trip(parity_out_dir):
    """Every vector's NODE canonical value must survive serialization (B4).

    ``vectors.mjs`` compares an IN-MEMORY value, so it cannot see the boundary
    the real Node leg crosses: ``JSON.stringify({h: 1.0})`` emits ``{"h":1}``
    and ``json.load`` returns a Python ``int`` where the python and cli legs
    hold a ``float``. This test writes the JS canonical values THROUGH
    ``JSON.stringify``, re-reads them in Python and compares with the same
    ``canonical_equal`` the comparator uses.
    """
    if shutil.which("node") is None:
        if _require_node_strict():
            _fail(f"`node` is not on PATH but {REQUIRE_NODE_ENV}=1")
        pytest.skip("`node` is not on PATH")
    target = parity_out_dir / "node.vectors.json"
    proc = subprocess.run(
        ["node", str(NODE_VECTORS), "--emit", str(target)],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert proc.returncode == 0, f"emit failed:\n{proc.stdout}\n{proc.stderr}"
    payload = json.loads(target.read_text(encoding="utf-8"))
    data = load_vector_file()
    mismatches: List[str] = []
    for section in ("vectors", "rows"):
        emitted = {e["name"]: e["canonical"] for e in payload[section]}
        # `rows` cases without a `node` leg are not emitted; everything else is.
        expected_cases = [c for c in data[section] if section == "vectors" or "node" in c]
        assert len(emitted) == len(expected_cases), (
            f"emitted {len(emitted)} {section} for {len(expected_cases)} cases"
        )
        for case in expected_cases:
            got = emitted[case["name"]]
            if not canonical_equal(got, case["canonical"]):
                mismatches.append(
                    f"{section}/{case['name']}: expected {case['canonical']!r} "
                    f"({shape_tag(case['canonical'])}), round-tripped {got!r} ({shape_tag(got)})"
                )
            # A hostile column NAME must survive serialization as an OWN key.
            missing = [c for c in case.get("columns", {}) if c not in got]
            if missing:
                mismatches.append(
                    f"{section}/{case['name']}: column(s) {missing} absent after the JSON "
                    f"round trip (got {sorted(got)})"
                )
    assert not mismatches, (
        "JS canonical values changed across the JSON boundary:\n" + "\n".join(mismatches)
    )


@pytest.mark.slow
@pytest.mark.parametrize("fixture", FIXTURES, ids=FIXTURE_IDS)
def test_three_way_parity(fixture, parity_cli_binary, parity_out_dir, capsys):
    """Python binding == Node binding == CLI, as canonical JSON, per row."""
    skip_if_no_datasets()
    schema = PROJECT_ROOT / fixture["schema"]
    # UNCONDITIONAL (B1/#3148): a committed schema is never legitimately absent,
    # and conftest's skip_if_no_schema would drop this fixture silently.
    assert schema.is_file(), f"committed schema absent: {schema}"

    out_dir = parity_out_dir
    legs: Dict[str, List[dict]] = {}
    payloads: Dict[str, dict] = {}
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
    payloads["python"] = py_payload

    # --- CLI leg -------------------------------------------------------------
    try:
        cli_payload = run_cli_leg(fixture, parity_cli_binary, out_dir)
    except Exception as exc:  # noqa: BLE001
        _fail(f"cli leg failed for {fixture['name']!r}: {type(exc).__name__}: {exc}")
    legs["cli"] = cli_payload["rows"]
    payloads["cli"] = cli_payload

    # --- Node leg ------------------------------------------------------------
    action, message = node_leg_disposition(_node_available(), _require_node_strict())
    if action == "run":
        try:
            node_payload = run_node_leg(fixture, out_dir)
        except Exception as exc:  # noqa: BLE001
            _fail(f"node leg failed for {fixture['name']!r}: {type(exc).__name__}: {exc}")
        legs["node"] = node_payload["rows"]
        payloads["node"] = node_payload
    elif action == "fail":
        _fail(message)
    else:
        notes.append(message)

    for leg, payload in payloads.items():
        assert_leg_columns(leg, payload, declared)
    assert_union_columns(payloads, declared)

    # Every run STATES which legs it compared and every declared gap: a lane
    # that omits coverage silently is indistinguishable from one that covers it.
    header = (
        f"[cross-binding parity] fixture={fixture['name']} "
        f"legs_compared={sorted(legs)} rows={len(legs['python'])} "
        f"columns={len(declared)}"
    )
    with capsys.disabled():
        print("\n" + header)
        for note in notes:
            print(f"[cross-binding parity] {note}")
        print(f"[cross-binding parity] DECLARED GAPS ({len(DECLARED_GAPS)}, NON-EXHAUSTIVE):")
        for gap in DECLARED_GAPS:
            print(f"[cross-binding parity]   - {gap}")

    failures = compare_legs(legs)
    assert not failures, (
        f"{header}\n" + "\n".join(notes) + ("\n" if notes else "") + "\n".join(failures)
    )


# ---------------------------------------------------------------------------
# The fail-closed switches (issue #1455, B5)
# ---------------------------------------------------------------------------


def test_node_leg_disposition_runs_when_available():
    assert node_leg_disposition(None, False) == ("run", None)
    assert node_leg_disposition(None, True) == ("run", None)


def test_node_leg_disposition_fails_closed_under_strict():
    action, message = node_leg_disposition("`node` is not on PATH", True)
    assert action == "fail"
    assert "`node` is not on PATH" in message and REQUIRE_NODE_ENV in message


def test_node_leg_disposition_declares_the_omission_otherwise():
    action, message = node_leg_disposition("no built native module", False)
    assert action == "omit"
    assert "DECLARED OMISSION" in message
    assert "2 of 3 legs" in message
    assert REQUIRE_NODE_ENV in message


@pytest.mark.parametrize("value,expected", [("1", True), ("true", True), ("0", False), ("", False)])
def test_require_node_strict_reads_the_environment(monkeypatch, value, expected):
    monkeypatch.setenv(REQUIRE_NODE_ENV, value)
    assert _require_node_strict() is expected


def test_require_node_strict_is_false_when_unset(monkeypatch):
    monkeypatch.delenv(REQUIRE_NODE_ENV, raising=False)
    assert _require_node_strict() is False


def test_node_available_reports_a_reason_when_node_is_absent(monkeypatch):
    monkeypatch.setattr(shutil, "which", lambda _name: None)
    assert _node_available() == "`node` is not on PATH"


def test_strict_switches_reach_the_disposition(monkeypatch):
    """Env plumbing + decision, together — the pair the CI job depends on."""
    monkeypatch.setattr(shutil, "which", lambda _name: None)
    monkeypatch.setenv(REQUIRE_NODE_ENV, "1")
    assert node_leg_disposition(_node_available(), _require_node_strict())[0] == "fail"
    monkeypatch.delenv(REQUIRE_NODE_ENV, raising=False)
    monkeypatch.delenv("CQLITE_REQUIRE_FIXTURES", raising=False)
    assert node_leg_disposition(_node_available(), _require_node_strict())[0] == "omit"


def _raise_skip():
    pytest.skip("Failed to build CLI: cargo exploded")


def test_parity_cli_binary_fails_closed_under_strict_fixtures(monkeypatch):
    """A skipped CLI leg under strict mode is a green run that compared NOTHING."""
    monkeypatch.setenv("CQLITE_REQUIRE_FIXTURES", "1")
    monkeypatch.delenv(REQUIRE_NODE_ENV, raising=False)
    with pytest.raises(pytest.fail.Exception, match="compared nothing"):
        resolve_parity_cli_binary(_raise_skip)


def test_parity_cli_binary_fails_closed_under_strict_node(monkeypatch):
    monkeypatch.delenv("CQLITE_REQUIRE_FIXTURES", raising=False)
    monkeypatch.delenv("CQLITE_PARITY_REQUIRE_DATASETS", raising=False)
    monkeypatch.setenv(REQUIRE_NODE_ENV, "1")
    with pytest.raises(pytest.fail.Exception, match="compared nothing"):
        resolve_parity_cli_binary(_raise_skip)


def test_parity_cli_binary_still_skips_for_local_dev(monkeypatch):
    """Local dev without a release build keeps the lenient behaviour."""
    monkeypatch.delenv("CQLITE_REQUIRE_FIXTURES", raising=False)
    monkeypatch.delenv("CQLITE_PARITY_REQUIRE_DATASETS", raising=False)
    monkeypatch.delenv(REQUIRE_NODE_ENV, raising=False)
    with pytest.raises(pytest.skip.Exception):
        resolve_parity_cli_binary(_raise_skip)


def test_parity_cli_binary_fails_closed_on_a_missing_binary_path(monkeypatch, tmp_path):
    """conftest can return a path that does not exist; strict mode must red."""
    monkeypatch.setenv("CQLITE_REQUIRE_FIXTURES", "1")
    ghost = tmp_path / "cqlite"
    with pytest.raises(pytest.fail.Exception, match="does not exist"):
        resolve_parity_cli_binary(lambda: ghost)


def test_parity_cli_binary_returns_an_existing_path(monkeypatch, tmp_path):
    monkeypatch.setenv("CQLITE_REQUIRE_FIXTURES", "1")
    real = tmp_path / "cqlite"
    real.write_text("#!/bin/sh\n", encoding="utf-8")
    assert resolve_parity_cli_binary(lambda: real) == real


def test_require_fixtures_strict_reads_the_environment(monkeypatch):
    """Behavioural, not a `callable()` tautology (issue #1455, N1)."""
    monkeypatch.delenv("CQLITE_REQUIRE_FIXTURES", raising=False)
    monkeypatch.delenv("CQLITE_PARITY_REQUIRE_DATASETS", raising=False)
    assert _require_fixtures_strict() is False
    monkeypatch.setenv("CQLITE_PARITY_REQUIRE_DATASETS", "1")
    assert _require_fixtures_strict() is True


# ---------------------------------------------------------------------------
# Negative controls -- proves the comparator CAN fail
# ---------------------------------------------------------------------------


def test_comparator_detects_value_divergence():
    a = [{"id": "aaaa", "n": 1}]
    b = [{"id": "aaaa", "n": 2}]
    failures = compare_legs({"python": a, "node": b, "cli": a})
    assert failures, "comparator accepted two legs with a different value"
    assert "column 'n'" in failures[0]


def test_comparator_detects_type_divergence_without_value_divergence():
    """``1`` and ``"1"`` must NOT compare equal, and neither must ``1`` and ``True``.

    This is the integer rule's whole point: a leg emitting a big integer as a
    JSON number while another emits a decimal string is a real divergence, and
    a plain ``==`` in Python would not catch the bool/int case.
    """
    failures = compare_legs({"python": [{"n": 1}], "node": [{"n": "1"}]})
    assert failures, "comparator accepted an int on one leg and a string on another"
    failures = compare_legs({"python": [{"b": True}], "node": [{"b": 1}]})
    assert failures, "comparator accepted True on one leg and 1 on another"


def test_comparator_accepts_an_integral_float_across_the_json_boundary():
    """The inverse control for B4: ``2`` and ``2.0`` are ONE canonical value.

    JSON has a single number type and the Node leg is read back through
    ``json.load``, so distinguishing them would red the lane on correct input.
    """
    assert not compare_legs({"python": [{"h": 2.0}], "node": [{"h": 2}]})


def test_comparator_detects_row_count_divergence():
    failures = compare_legs({"python": [{"n": 1}], "node": [{"n": 1}, {"n": 2}]})
    assert failures and "row COUNT differs" in failures[0]


def test_comparator_detects_row_order_divergence():
    """Row ORDER is compared, never sorted away.

    Measured on this corpus: all three legs return rows in the SAME order (500
    rows of test_basic.simple_table as well as every fixture), so an order
    difference is a real finding and the comparator must surface it.
    """
    a = [{"id": "x"}, {"id": "y"}]
    failures = compare_legs({"python": a, "node": list(reversed(a))})
    assert failures and "row 0" in failures[0]


def test_comparator_treats_absent_column_as_null_not_a_difference():
    """The NODE leg omits a column with no value; the CLI always emits it."""
    failures = compare_legs({"node": [{"id": "x"}], "cli": [{"id": "x", "extra": None}]})
    assert not failures, failures


def test_comparator_still_fails_when_the_present_side_is_not_null():
    failures = compare_legs({"node": [{"id": "x"}], "cli": [{"id": "x", "extra": 5}]})
    assert failures and "column 'extra'" in failures[0]


def test_comparator_rejects_zero_rows_on_every_leg():
    failures = compare_legs({"python": [], "node": [], "cli": []})
    assert failures and "0 rows" in failures[0]


def test_comparator_needs_at_least_two_legs():
    assert compare_legs({"python": [{"n": 1}]})


def test_comparator_detects_nested_divergence():
    """A difference inside a collection must not be swallowed."""
    failures = compare_legs(
        {"python": [{"m": [["a", 1]]}], "node": [{"m": [["a", 2]]}]}
    )
    assert failures and "column 'm'" in failures[0]


# ---------------------------------------------------------------------------
# Column-set rules (issue #1455, B3)
# ---------------------------------------------------------------------------


def _payload(leg: str, columns: List[str]) -> dict:
    return {"fixture": "f", "leg": leg, "observed_columns": columns}


def test_leg_columns_allow_an_omitted_column():
    assert_leg_columns("node", _payload("node", ["a"]), ["a", "b"])


def test_leg_columns_reject_an_undeclared_column():
    with pytest.raises(AssertionError, match="not declared"):
        assert_leg_columns("cli", _payload("cli", ["a", "zzz"]), ["a", "b"])


def test_union_columns_reject_a_column_no_leg_returns():
    with pytest.raises(AssertionError, match="returned by NO leg"):
        assert_union_columns({"python": _payload("python", ["a"])}, ["a", "b"])


def test_union_columns_accept_a_column_only_one_leg_returns():
    assert_union_columns(
        {"python": _payload("python", ["a", "b"]), "node": _payload("node", ["a"])},
        ["a", "b"],
    )
