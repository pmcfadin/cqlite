"""A ROW COLUMN named `__proto__` is an ordinary dict key in Python (issue #3630).

AC8 of #3630: *"The Python binding's row path is checked for the analogous hole in
the same change (a Python `dict` has no inherited accessors, so the expectation is
'no defect' — but it should be asserted, not assumed)."*

**This file changes no Python behaviour. It exists to turn a prediction into a
measurement.** The Node defect is that `row_to_object` wrote user-controlled
column names through a JavaScript `[[Set]]`, which consults the prototype chain,
so a column named `__proto__` reached `Object.prototype`'s inherited ACCESSOR
instead of becoming a property — silently losing the column, or replacing the
object's prototype when the value was null.

**Why Python is structurally immune, and why that is still worth asserting.**
`dict.__setitem__` performs a hash-table insert. It does not consult the type's
MRO, there is no inherited-accessor mechanism for string keys, and `dict` exposes
no `__proto__`-like attribute that a key could collide with — `"__proto__"`,
`"constructor"` and `"toString"` are unremarkable strings to a `dict`. So the
expectation is that all four collision columns arrive intact. The reason to assert
it anyway is that "structurally immune" is an argument about the language, and the
row path is CQLite's code: a future change that swapped the row `dict` for an
attribute-style object, a `SimpleNamespace`, or anything using `setattr` would
reintroduce exactly this class, and nothing else in the suite would notice.

THE SUBJECT IS CASSANDRA-5.0.2-WRITTEN — `test-data/fixtures/issue_3630/`, from
`test-data/scripts/generate-issue-3630-row-collision.sh`. Shared with the Node
suite (`bindings/node/__test__/issue-3630-row-key-namespace.test.js`) so both
bindings are measured against the SAME bytes, which is the only way "the two
bindings agree" is a claim rather than a hope.

Committed source, so absence is a BROKEN CHECKOUT: every path here fails closed
and this module must never skip.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

import cqlite

from conftest import PROJECT_ROOT, SCHEMAS

FIXTURE_ROOT = PROJECT_ROOT / "test-data" / "fixtures" / "issue_3630"
SCHEMA = SCHEMAS / "issue-3630-row-collision.cql"
QUERY = "SELECT * FROM test_row_collision.row_collide"

#: The one column name that is intercepted in JavaScript — the only inherited
#: ACCESSOR on `Object.prototype`. In Python it is just a string.
ACCESSOR_COL = "__proto__"
#: Inherited WRITABLE DATA properties in JavaScript (so a `[[Set]]` of them
#: already worked there); the class discriminator. Ordinary strings here too.
INHERITED_DATA_COLS = ("constructor", "toString")
#: Not on `Object.prototype` at all — it lives on functions. No mechanism in
#: either language; present because the ruling is to enumerate the class.
NON_INHERITED_COL = "prototype"
ALL_COLLISION_COLS = (ACCESSOR_COL, *INHERITED_DATA_COLS, NON_INHERITED_COL)


def _assert_fixture_present() -> None:
    """Fail closed, naming the missing artifact, on a checkout that lacks it."""
    assert SCHEMA.is_file(), f"committed schema missing: {SCHEMA}"
    # GLOB the table directory: a regeneration mints a new UUID, so a hardcoded
    # path would rot the first time the fixture is rebuilt.
    tables = sorted(FIXTURE_ROOT.glob("test_row_collision/row_collide-*"))
    assert len(tables) == 1, (
        f"expected exactly one row_collide-* table dir under {FIXTURE_ROOT}, got {tables}"
    )
    data_db = sorted(tables[0].glob("*-Data.db"))
    assert data_db, (
        f"no *-Data.db under {tables[0]} — the binaries are gitignored and must be "
        "force-added (`git add -f`); see test-data/fixtures/issue_3630/README.md"
    )


@pytest.fixture(scope="module")
def rows() -> dict[int, dict[str, Any]]:
    """Every row of the fixture table, keyed by `id`, read through the public API."""
    _assert_fixture_present()
    with cqlite.open(FIXTURE_ROOT, schema=SCHEMA) as db:
        result = db.execute(QUERY)
        by_id = {row.get("id"): row.to_dict() for row in result.rows}
    # Three rows by construction. A partial read is a decode regression, not a
    # reason to assert less.
    assert sorted(by_id) == [1, 2, 3], f"expected rows 1..3, got {sorted(by_id)}"
    return by_id


def test_string_valued_proto_column_is_an_ordinary_key(rows) -> None:
    """The case that VANISHES in unfixed JavaScript arrives intact in Python."""
    row = rows[1]
    assert row[ACCESSOR_COL] == "user-supplied-proto"
    # Asserted as a KEY SET, never as a count: a count states only "N of
    # something" and cannot see a column lost while another took its place,
    # which is this defect's entire shape.
    assert set(row) == {"id", *ALL_COLLISION_COLS, "real_col"}


def test_every_collision_column_arrives_on_the_populated_row(rows) -> None:
    """All four names, so the claim covers the CLASS and not one name."""
    row = rows[1]
    assert row["constructor"] == "user-supplied-constructor"
    assert row["toString"] == "user-supplied-tostring"
    assert row[NON_INHERITED_COL] == "user-supplied-prototype"
    assert row["real_col"] == 42


def test_the_row_is_a_plain_dict_with_no_attribute_shadowing(rows) -> None:
    """A key never becomes an attribute, so no key can shadow dict's own API.

    This is the assertion that would FAIL if the row path were ever changed to an
    attribute-style object — the only realistic way this defect class could reach
    Python.
    """
    row = rows[1]
    assert isinstance(row, dict)
    # `row["toString"]` is data; `row.keys` is still dict's method, unshadowed.
    assert callable(row.keys)
    assert callable(row.items)
    # And the collision keys are reachable ONLY as keys, never as attributes.
    for col in ALL_COLLISION_COLS:
        assert not hasattr(row, col), (
            f"{col!r} became an ATTRIBUTE of the row object; the row path is no "
            "longer a plain mapping and #3630's defect class is reachable in Python"
        )


def test_null_valued_proto_column_changes_nothing(rows) -> None:
    """The null case — harmless in Python, and the SHAPE DIFFERS FROM NODE'S.

    In JavaScript this same input is the harsher failure mode: assigning null to
    `__proto__` REPLACES the object's prototype. Python has no analogue — there is
    nothing a string key can assign to — so the only thing to assert is that the
    value arrives and the mapping is unremarkable.

    **MEASURED CROSS-BINDING DIVERGENCE, and this test asserts PYTHON'S ACTUAL
    behaviour rather than Node's.** Row 2's explicit CQL NULL is a cell tombstone
    with no value cell. The two bindings render that differently:

    * **Node SKIPS** it — the column is absent from the row object. Deliberate,
      and documented in `row_to_object`: null-filling would emit a phantom
      `col_0: null` beside a real cell for aggregate queries, where core's
      metadata name and the value key differ (#1446).
    * **Python NULL-FILLS** it — the key is present with `None`.

    Neither is obviously wrong and this change does not alter either. It is
    asserted here, rather than assumed away, because the first draft of this file
    asserted ABSENCE — carried over from the Node measurement — and FAILED. That
    failure is the finding: the bindings disagree on a row's KEY SET for any
    declared-but-valueless column, which is a semantic difference a consumer
    porting between them would hit immediately. Recorded in
    `.phase2-measurements.md` and reported for a follow-up issue; it is orthogonal
    to #3630's prototype class and deliberately NOT fixed here.
    """
    row = rows[2]
    # Python's shape: the tombstoned column is PRESENT with a None value.
    assert ACCESSOR_COL in row
    assert row[ACCESSOR_COL] is None
    # The siblings on the same row are unaffected.
    assert row["constructor"] == "user-supplied-constructor-2"
    assert row["toString"] == "user-supplied-tostring-2"
    assert row[NON_INHERITED_COL] == "user-supplied-prototype-2"
    assert isinstance(row, dict)


def test_contrast_row_has_no_collision_columns(rows) -> None:
    """Row 3: no collision cell on disk — every declared column is None-filled.

    Same divergence as the test above: Node yields `{"id", "real_col"}` for this
    row, Python yields all six keys with `None` for the four unset ones. Asserted
    as PYTHON's measured shape.

    What this row still pins, and it is the point of having it: the key set is a
    property of the SCHEMA, not of the values, so no collision column name is
    treated specially in either direction — a `__proto__` key appears here for
    exactly the same reason `real_col` does.
    """
    row = rows[3]
    assert set(row) == {"id", "real_col", *ALL_COLLISION_COLS}
    for col in ALL_COLLISION_COLS:
        assert row[col] is None, f"{col!r} should be None-filled on the contrast row"
    assert row["real_col"] == 44
