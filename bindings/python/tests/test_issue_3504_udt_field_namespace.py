"""A UDT field named `_type`/`_keyspace` displaces nothing (issue #3504).

Spec: `openspec/changes/udt-field-namespace/specs/udt-type-identity/spec.md`.

Both bindings used to render a UDT as ONE flat namespace holding the injected
type identity (`_type`, `_keyspace`) and the UDT's own declared field names,
markers written first. A UDT that DECLARES a field named `_type` or `_keyspace`
— legal CQL via a quoted identifier — therefore silently OVERWROTE the marker,
and the type name became unrecoverable from the result. Python now returns a
`cqlite.Udt` carrying `type_name`/`keyspace` out of band, with `fields` holding
declared fields and nothing else.

THE SUBJECT IS CASSANDRA-WRITTEN. `test-data/fixtures/issue_3504/` is produced by
`test-data/scripts/generate-issue-3504-udt-collision.sh` inside a cassandra:5.0.2
container, not by CQLite's write path. That matters twice over: it proves the
DECODER can produce such a UDT at all (a constructed in-memory `Value::Udt`
could not), and CQLite's write path cannot emit a UDT literal anyway.

WHAT THE COMMITTED JSONL GOLDEN IS *NOT* AN ORACLE FOR. For this input
`sstabledump`'s flat `{"_type": "user-supplied-type", ...}` is TEXTUALLY
IDENTICAL to what the old buggy binding injection produced, so physical-dump
parity is structurally blind to the rendering defect and must not be cited as
evidence the fix works. The oracle here is the spec's required shape, asserted at
the binding surface. Corroboration worth stating, though: sstabledump injects no
marker of its own — the non-colliding `p` cell dumps as
`{"label": ..., "real_field": 7}` — so the authoritative reference tool already
keeps type identity out of the field namespace, which is what this change does.

WHY THIS FILE RESOLVES ITS OWN PATHS. `conftest.DATASETS` is an EITHER/OR on
`CQLITE_DATASETS_ROOT` (`conftest.py:42-48`): unset, it DOES fall back to the
checkout's `test-data/datasets/sstables`. But when the variable IS set — which
every gate run does — the checkout copy is never consulted, so a fixture reached
through `DATASETS` would be INVISIBLE exactly where it has to run. The fixture is committed checkout-relative and is resolved from
`PROJECT_ROOT` with no environment variable. `PROJECT_ROOT`/`SCHEMAS` are
themselves checkout-derived in `conftest`, so importing them introduces no env
dependency.

FAIL-CLOSED, NEVER SKIP. Every path below is git-committed source, so absence is
a broken checkout and not a skippable condition (CLAUDE.md #3220: committed
fixtures are `must_run`).
"""

from __future__ import annotations

import json
from pathlib import Path
from types import MappingProxyType
from typing import Any

import pytest

import cqlite

from conftest import PROJECT_ROOT, SCHEMAS

FIXTURE_ROOT = PROJECT_ROOT / "test-data" / "fixtures" / "issue_3504"
SCHEMA = SCHEMAS / "issue-3504-udt-collision.cql"
PARITY_FACTS = FIXTURE_ROOT / "binding-parity-facts.json"
QUERY = "SELECT * FROM test_udt_collision.udt_collide"

# The SECOND fixture table (roborev R1-2): one column per side of the
# hashable-projection totality boundary. Queried ROW BY ROW, because a row whose
# projection still raises would otherwise hide the rows that now succeed.
SHAPES_TABLE = "test_udt_collision.udt_hashable_shapes"

# The `collide` value the shapes table stores inside its tuples, as the fields
# mapping the projection must produce. Written once: five assertions depend on
# it, and a copy per assertion is how one of them ends up silently weaker.
TUPLE_UDT_FIELDS = {
    "_type": "tuple-type-marker",
    "_keyspace": "tuple-keyspace-marker",
    "__proto__": "tuple-proto-marker",
    "real_field": 300,
}


def _assert_fixture_present() -> None:
    """Fail closed, naming the missing artifact, on a checkout that lacks it."""
    assert SCHEMA.is_file(), f"committed schema missing: {SCHEMA}"
    assert PARITY_FACTS.is_file(), f"committed parity reference missing: {PARITY_FACTS}"
    # GLOB the table directory: a regeneration mints a new UUID, so a hardcoded
    # path would rot the first time the fixture is rebuilt.
    tables = sorted(FIXTURE_ROOT.glob("test_udt_collision/udt_collide-*"))
    assert len(tables) == 1, (
        f"expected exactly one udt_collide-* table dir under {FIXTURE_ROOT}, got {tables}"
    )
    data_db = sorted(tables[0].glob("*-Data.db"))
    assert data_db, (
        f"no *-Data.db under {tables[0]} — the binaries are gitignored and must be "
        "force-added (`git add -f`); see test-data/fixtures/issue_3504/README.md"
    )


@pytest.fixture(scope="module")
def rows() -> dict[int, dict[str, Any]]:
    """Every row of the fixture table, keyed by `id`, read through the public API."""
    _assert_fixture_present()
    with cqlite.open(FIXTURE_ROOT, schema=SCHEMA) as db:
        result = db.execute(QUERY)
        by_id = {row.get("id"): row.to_dict() for row in result.rows}
    # The fixture has three rows by construction; a partial read is a decode
    # regression, not a reason to assert less.
    assert sorted(by_id) == [1, 2, 3], f"unexpected fixture row ids: {sorted(by_id)}"
    return by_id


def _shapes_row(row_id: int) -> dict[str, Any]:
    """One row of the shapes table, read through the public API.

    Opened per call rather than shared: three of these reads are expected to
    RAISE, and a session-scoped fixture would make the first failure the whole
    module's error instead of one test's assertion.
    """
    with cqlite.open(FIXTURE_ROOT, schema=SCHEMA) as db:
        result = db.execute(f"SELECT * FROM {SHAPES_TABLE} WHERE id = {row_id}")
        assert len(result.rows) == 1, (
            f"shapes table row {row_id} not found — the fixture is incomplete"
        )
        return result.rows[0].to_dict()


def _facts(udt: cqlite.Udt) -> dict[str, Any]:
    """The language-neutral fact triple for `udt` (the committed reference's shape)."""
    return {
        "typeName": udt.type_name,
        "keyspace": udt.keyspace,
        "fields": dict(udt.fields),
    }


# =============================================================================
# Site 3 — the rendered UDT
# =============================================================================


def test_colliding_udt_yields_both_the_field_and_the_identity(rows):
    """SCENARIO: a UDT with both colliding field names round-trips.

    Both the FIELD values and the type identity are recoverable, and neither
    overwrites the other — which is the whole requirement.
    """
    udt = rows[1]["c"]
    assert isinstance(udt, cqlite.Udt), f"expected cqlite.Udt, got {type(udt).__name__}"

    # Identity, from a namespace no field name can address.
    assert udt.type_name == "collide"
    assert udt.keyspace == "test_udt_collision"

    # ...and the declared fields, all four, unmodified.
    assert dict(udt.fields) == {
        "_type": "user-supplied-type",
        "_keyspace": "user-supplied-keyspace",
        "__proto__": "user-supplied-proto",
        "real_field": 42,
    }
    # The exact field-NAME SET, with NO injected entries. A COUNT would be a
    # weaker assertion for the reason the Node side measured: under the old shape
    # the count was right while a field was missing (an injected key had taken
    # its place), so the names are what have to be asserted. The count is kept
    # only as a redundant statement of the same fact.
    assert set(udt.keys()) == {"_type", "_keyspace", "__proto__", "real_field"}
    assert len(udt) == 4

    # Mapping access reaches the FIELD, never the marker.
    assert udt["_type"] == "user-supplied-type"
    assert udt["_keyspace"] == "user-supplied-keyspace"
    assert udt["real_field"] == 42
    assert "_type" in udt and "_keyspace" in udt
    assert sorted(udt.keys()) == ["__proto__", "_keyspace", "_type", "real_field"]
    assert sorted(udt.items()) == sorted(dict(udt.fields).items())
    assert list(iter(udt)) == list(udt.fields)


def test_non_colliding_udt_keeps_working_through_field_access(rows):
    """SCENARIO: a non-colliding UDT keeps working through field access."""
    udt = rows[1]["p"]
    assert isinstance(udt, cqlite.Udt)
    assert udt.type_name == "plain"
    assert udt.keyspace == "test_udt_collision"
    assert udt["label"] == "no-colliding-field"
    assert udt.fields["label"] == "no-colliding-field"
    assert "real_field" in udt
    assert dict(udt.fields) == {"label": "no-colliding-field", "real_field": 7}
    assert sorted(udt.keys()) == ["label", "real_field"]
    assert len(udt) == 2


def test_the_marker_is_no_longer_readable_from_the_field_namespace(rows):
    """SCENARIO: the marker is no longer readable from the field namespace.

    This is the REMOVED SHARED CHANNEL, asserted as removed rather than left as
    an incidental regression: on `main` `udt["_type"]` returned the type name for
    this exact UDT.
    """
    udt = rows[1]["p"]
    assert "_type" not in udt
    assert "_keyspace" not in udt
    with pytest.raises(KeyError):
        _ = udt["_type"]
    with pytest.raises(KeyError):
        _ = udt["_keyspace"]
    # ...and the identity is still there, out of band.
    assert udt.type_name == "plain"
    assert udt.keyspace == "test_udt_collision"


def test_a_null_colliding_field_does_not_null_the_type_name(rows):
    """A NULL `_type` FIELD is a second, distinct failure mode from the string case.

    Under the old code the injected type name was overwritten by whatever the
    field held — so a null `_type` field made the type name `None`, not merely
    wrong. Both halves are asserted: the identity survives AND the null field is
    reported as null rather than dropped.
    """
    udt = rows[3]["c"]
    assert isinstance(udt, cqlite.Udt)
    assert udt.type_name == "collide", "a NULL `_type` field must not null the type name"
    assert udt.keyspace == "test_udt_collision"
    assert udt.fields["_type"] is None
    assert udt["_type"] is None
    assert "_type" in udt, "a null-valued field is still a DECLARED field"
    assert dict(udt.fields) == {
        "_type": None,
        "_keyspace": "keyspace-field-only",
        "__proto__": None,
        "real_field": 0,
    }
    assert set(udt.keys()) == {"_type", "_keyspace", "__proto__", "real_field"}
    assert len(udt) == 4


# =============================================================================
# Site 4 — the hashable projection (`value_to_hashable_key`)
# =============================================================================


def test_projected_map_key_holds_exactly_one_type_entry(rows):
    """SCENARIO: a field named `_type` no longer yields a duplicate pair.

    The subject is `fcm` — `frozen<map<frozen<collide>, int>>`. Its multicell
    sibling `cm` reaches the same projection since #3612 (pinned below, asserted
    against this very case); before that fix a non-frozen map's cell-path key
    decoded to `Value::Blob` and never reached the projection at all.

    On `main` the projection flattened the UDT into a `frozenset` holding a pair
    for `_type` (the injected type name), a pair for `_keyspace`, and then one per
    field — so this key carried TWO `_type` pairs with different values and
    nothing deduped them. The projection now carries identity out of band, so the
    pair set holds exactly one entry per declared field and none for the metadata.
    """
    fcm = rows[1]["fcm"]
    assert isinstance(fcm, dict) and len(fcm) == 1
    key, value = next(iter(fcm.items()))
    assert value == 3

    assert isinstance(key, cqlite.Udt), (
        f"a frozen map's UDT key must project to cqlite.Udt, got {type(key).__name__}"
    )
    # EXACTLY ONE `_type` entry — the field's — counted over the pair set rather
    # than probed by membership, because the defect was a DUPLICATE pair and a
    # membership test cannot see one.
    names = [name for name, _ in key.items()]
    assert names.count("_type") == 1, f"duplicate/absent `_type` entry: {names}"
    assert names.count("_keyspace") == 1
    assert names.count("__proto__") == 1
    assert sorted(names) == ["__proto__", "_keyspace", "_type", "real_field"]
    assert key.fields["_type"] == "key-type-marker"
    assert key.fields["_keyspace"] == "key-keyspace-marker"

    # ...and the identity is recoverable from the projected key WITHOUT reading
    # the field namespace.
    assert key.type_name == "collide"
    assert key.keyspace == "test_udt_collision"


def test_same_fields_different_udt_types_stay_distinct_projected_keys(rows):
    """SCENARIO: same fields, different UDT types stay distinct.

    `fcm`'s key (`collide`) and `ftm`'s key (`collide_twin`) have IDENTICAL field
    names and values and differ only in declared type. The old `frozenset`
    projection told them apart solely because it injected `_type`/`_keyspace`
    pairs into the pair set; removing those pairs — the point of this change —
    would have collapsed the two into one key had identity not moved onto the
    instance. So this is the property that keeps the fix from being a regression.
    """
    (collide_key, _) = next(iter(rows[1]["fcm"].items()))
    (twin_key, _) = next(iter(rows[1]["ftm"].items()))

    assert dict(collide_key.fields) == dict(twin_key.fields), (
        "fixture precondition: the two keys must have identical fields"
    )
    assert collide_key.type_name == "collide"
    assert twin_key.type_name == "collide_twin"

    assert collide_key != twin_key
    assert hash(collide_key) != hash(twin_key)
    # The property that actually matters: as keys of ONE dict they stay two.
    assert len({collide_key: "a", twin_key: "b"}) == 2


def test_frozen_set_of_udt_elements_keep_their_identity(rows):
    """`frozen<set<frozen<collide>>>` elements are `cqlite.Udt`, identity intact.

    Note which code path this exercises, so it is not mistaken for a second
    site-4 case: `set_to_py` sees a UDT element (`contains_udt`) and returns a
    Python `list` of `value_to_py` values for CLI parity (#804), so the element
    comes from site 3, NOT from the hashable projection. The projection's set-side
    entry point is only reached by UDT-free sets.
    """
    fs = rows[1]["fs"]
    assert isinstance(fs, list) and len(fs) == 1
    member = fs[0]
    assert isinstance(member, cqlite.Udt)
    assert member.type_name == "collide"
    assert member.keyspace == "test_udt_collision"
    assert dict(member.fields) == {
        "_type": "set-member-type",
        "_keyspace": "set-member-keyspace",
        "__proto__": "set-member-proto",
        "real_field": 200,
    }


def test_non_frozen_map_udt_key_projects_like_the_frozen_control(rows):
    """FIXED (#3612): a MULTICELL map's UDT key now reaches the projection.

    This test used to pin the DEFECT. A NON-frozen `map<frozen<udt>, int>` is
    multicell, so its key lives in the CELL PATH, and `parse_cell_path_key`
    (`cqlite-core/src/storage/sstable/reader/parsing/row_decoder/complex_column/cell_path_key.rs`)
    used to match a closed set of PRIMITIVE cell-path types and fall back to
    `Value::Blob` for a frozen UDT — so `cm`/`tm` keys arrived as `bytes` and
    never reached the UDT projection at all. It now delegates to the structural
    decoder, so the key is a `cqlite.Udt` exactly as the FROZEN spelling's is.

    Asserted AGAINST the frozen control rather than against literals: `cm` and
    `fcm` are the two legal spellings of the same `map<frozen<collide>, int>` and
    the fixture stores the same key in both, so the strongest statement is that
    they project EQUAL. Mirrors `test_projected_map_key_holds_exactly_one_type_entry`
    (the `fcm` case) so both spellings visibly carry one contract.
    """
    for column, frozen_control, expected_type in (
        ("cm", "fcm", "collide"),
        ("tm", "ftm", "collide_twin"),
    ):
        cell = rows[1][column]
        assert isinstance(cell, dict) and len(cell) == 1, column
        key = next(iter(cell))
        assert isinstance(key, cqlite.Udt), (
            f"{column}: a multicell map's UDT key must project to cqlite.Udt, "
            f"got {type(key).__name__} (issue #3612)"
        )
        assert key.type_name == expected_type
        assert key.keyspace == "test_udt_collision"
        assert dict(key.fields) == {
            "_type": "key-type-marker",
            "_keyspace": "key-keyspace-marker",
            "__proto__": "key-proto-marker",
            "real_field": 100,
        }
        # The parity statement: the multicell and frozen spellings of one map
        # present the same key, so a caller cannot tell them apart.
        control_key = next(iter(rows[1][frozen_control]))
        assert key == control_key, (
            f"{column} vs {frozen_control}: the two spellings of one "
            "map<frozen<udt>, int> must project the SAME key"
        )
        assert hash(key) == hash(control_key)


# =============================================================================
# The `fields` view is READ-ONLY — the hash invariant `Udt` advertises
# =============================================================================


def test_fields_view_cannot_be_mutated_out_of_its_hash_bucket(rows):
    """SCENARIO: `udt.fields[...] = x` must not unseat a `Udt` used as a dict key.

    `Udt` is declared `frozen`, hashes over `(keyspace, type_name, fields)`, and
    is documented as usable as a `dict` key. A derived `#[pyo3(get)]` on the
    internal `Py<PyDict>` would hand back a new reference to the SAME dict that
    `__hash__` reads, so one item assignment through the getter would move an
    already-inserted key out of its bucket and `d[key]` would raise `KeyError` —
    the constructor's copy protects the CALLER's dict, not the value. Measured on
    the pre-fix build: the mutation was accepted and the lookup raised.

    The subject is a projected map key from the Cassandra-written fixture, i.e.
    the production `value_to_hashable_key` path where a `Udt` really is used as a
    dict key, not a constructed instance.
    """
    key = next(iter(rows[1]["fcm"]))
    assert isinstance(key, cqlite.Udt)
    before = dict(key.fields)
    hash_before = hash(key)

    holder = {key: "v"}
    assert holder[key] == "v"

    view = key.fields
    assert isinstance(view, MappingProxyType), (
        "the getter must not hand out the internal mutable dict; "
        f"got {type(view).__name__}"
    )
    # REFUSED, not silently discarded: a `dict` copy would accept these writes
    # and drop them, which reads as success.
    with pytest.raises(TypeError):
        view["z"] = 1
    with pytest.raises(TypeError):
        del view["_type"]
    # `mappingproxy` exposes no mutating methods at all, so the usual escape
    # hatches are AttributeError rather than a silent no-op.
    for method in ("update", "clear", "pop", "setdefault"):
        assert not hasattr(view, method), f"mappingproxy grew a mutator: {method}"

    assert dict(key.fields) == before
    assert hash(key) == hash_before
    assert holder[key] == "v", "the key escaped its hash bucket"


def test_fields_view_supports_every_read_shape_callers_use(rows):
    """The read-only view is a drop-in for the `dict` it replaced.

    Enumerated rather than asserted in passing, because choosing a proxy over a
    `dict` copy is only safe if every consuming shape survives it: this is the
    claim, measured.
    """
    udt = rows[1]["c"]
    view = udt.fields

    assert dict(view) == dict(udt.items())
    assert view["_type"] == "user-supplied-type"
    assert sorted(view.items()) == sorted(udt.items())
    assert list(view.keys()) == list(udt.keys())
    assert list(view.values()) == list(udt.values())
    assert "_keyspace" in view and "nope" not in view
    assert len(view) == len(udt)
    assert list(iter(view)) == list(iter(udt))
    # Equality against a plain `dict` holds in both directions, so an existing
    # `assert udt.fields == {...}` assertion keeps its meaning.
    assert view == dict(view) and dict(view) == view
    # ...and `.copy()` yields a genuinely mutable `dict`, the documented escape.
    mutable = view.copy()
    mutable["z"] = 1
    assert "z" not in udt.fields


def test_constructed_udt_fields_view_is_read_only_too():
    """The same property on the public constructor, without the fixture.

    `cqlite.Udt(...)` is public API (it is what `value_to_hashable_key` returns
    and what a caller can build for comparison), so the guarantee cannot depend
    on the value having come from a decode.
    """
    udt = cqlite.Udt("address", "ks", {"street": "1 Main St"})
    holder = {udt: "v"}
    with pytest.raises(TypeError):
        udt.fields["street"] = "moved"
    assert holder[udt] == "v"
    assert udt.fields["street"] == "1 Main St"

    # The constructor's own copy still holds: mutating the CALLER's dict is inert.
    source = {"street": "1 Main St"}
    other = cqlite.Udt("address", "ks", source)
    source["street"] = "moved"
    assert other.fields["street"] == "1 Main St"
    assert other == udt


def test_eq_against_a_foreign_type_defers_rather_than_deciding():
    """`__eq__` returns `NotImplemented` for a non-`Udt` (nit N8).

    Observable consequence: a cooperating type's reflected `__eq__` is consulted.
    A hard `False` from `Udt.__eq__` would decide the comparison unilaterally, so
    this asserts the DEFERRAL, not just the `!=` a `False` would also produce.
    """

    class Anything:
        def __eq__(self, other):  # noqa: D105 - reflected side under test
            return True

        __hash__ = None

    udt = cqlite.Udt("address", "ks", {"street": "1 Main St"})
    assert udt == Anything(), "Udt.__eq__ must defer to the reflected __eq__"
    assert Anything() == udt
    # A type that does NOT cooperate still compares unequal.
    assert udt != {"street": "1 Main St"}
    assert udt != "address"


# =============================================================================
# Cross-binding parity (AC3)
# =============================================================================


def test_binding_facts_match_the_committed_cross_binding_reference(rows):
    """SCENARIO: the same UDT yields equal facts in both bindings.

    Compared as DATA, never by host type: this suite and
    `bindings/node/__test__/issue-3504-udt-field-namespace.test.js` each derive
    the same fact set from their OWN binding output and assert equality against
    ONE committed file, so neither can drift without reddening. The Python
    binding's `type_name` maps onto the reference's `typeName` — PyO3 exposes
    snake_case and napi-rs camelCases; the SEMANTICS, which is what AC3
    constrains, are identical.
    """
    reference = json.loads(Path(PARITY_FACTS).read_text())
    expected = reference["udts"]

    observed = {
        "row1.c": _facts(rows[1]["c"]),
        "row1.p": _facts(rows[1]["p"]),
        "row1.fcm_key": _facts(next(iter(rows[1]["fcm"]))),
        "row1.ftm_key": _facts(next(iter(rows[1]["ftm"]))),
        "row1.fs_0": _facts(rows[1]["fs"][0]),
        "row2.p": _facts(rows[2]["p"]),
        "row3.c": _facts(rows[3]["c"]),
    }

    # Both directions: a reference entry with no observed counterpart is as much a
    # drift as the reverse, and comparing only the intersection would let either
    # side quietly shrink.
    assert sorted(observed) == sorted(expected)
    assert observed == expected

    assert next(iter(rows[1]["fcm"].values())) == reference["map_values"]["row1.fcm_value"]
    assert next(iter(rows[1]["ftm"].values())) == reference["map_values"]["row1.ftm_value"]

    # Non-vacuity: the reference must actually carry the colliding subject, or an
    # emptied/renamed file would let this pass having compared nothing.
    assert expected["row1.c"]["fields"]["_type"] == "user-supplied-type"
    assert expected["row1.c"]["typeName"] == "collide"


# =============================================================================
# R1-2 — the hashable-projection TOTALITY boundary
#
# THIS SECTION HAS BEEN RE-DERIVED TWICE, BY THE TWO CHANGES THAT MOVED THE
# BOUNDARY, AND THE HISTORY IS THE POINT.
#
# When #3504 landed, `value_to_hashable_key` had arms for `List`, `Map`, `Frozen`
# and `Udt` only; `Tuple` and `Set` had NONE and fell through to `value_to_py`.
# Making a UDT a HASHABLE `cqlite.Udt` moved the boundary WITHOUT adding an arm,
# because what had made those fallthrough shapes unprojectable was the UDT being
# an unhashable `dict`. #3500 then made both functions TOTAL — every `Value`
# variant named, no `_ =>`, pinned by
# `#[deny(clippy::wildcard_enum_match_arm)]` — and made `contains_udt` a full
# subtree traversal, which moved it again and in a different way.
#
# MEASURED at each stage on the committed `udt_hashable_shapes` table (a point
# read per row; the "before #3504" column was taken with that commit's parent
# binding built into the same venv):
#
#   col | shape                                            | pre-3504 | 3504  | +3500
#   ----+--------------------------------------------------+----------+-------+------
#   stu | set<frozen<tuple<frozen<collide>, int>>>          | TypeError| frozen| list
#       |   "unhashable type: 'dict'"                       |          | set   |
#   mtu | map<frozen<tuple<frozen<collide>, int>>, int>     | TypeError| dict  | dict
#       |   "unhashable type: 'dict'"                       |          |       |
#   stn | set<frozen<tuple<frozen<unhashable_fields>,int>>> | TypeError| frozen| list
#       |   "unhashable type: 'dict'"                       |          | set   |
#   ssu | set<frozen<set<frozen<collide>>>>                | TypeError| Type- | list
#       |   "unhashable type: 'list'"                       |          | Error |
#
# WHY EACH COLUMN MOVED, because the two changes are not interchangeable:
#
#   * #3504 fixed `stu`/`mtu`/`stn` INCIDENTALLY — no arm was added; the
#     fall-through to `value_to_py` merely began yielding a hashable `tuple` of
#     `cqlite.Udt` instead of a `tuple` holding a `dict`. `ssu` was untouched,
#     because ITS cause was never the UDT: `set_to_py` renders a UDT-bearing set
#     as a Python `list` for CLI parity (#804), and a `list` is unhashable. The
#     error text — `'list'`, not `'dict'` — is what identifies the two causes
#     apart, and is why that row is measured rather than assumed.
#
#   * #3500 then changed the CONTAINER of the three set columns, deliberately
#     (its AC1 over AC5). `contains_udt` now traverses the whole subtree, so
#     `set_to_py` sees the UDT under the tuple / under the inner set and takes
#     its #804 `list` branch for the whole column. `ssu` stops raising for the
#     same reason. `mtu` is unchanged: a map KEY has no #804 branch to take —
#     `map_to_py` must project, so it still goes through
#     `value_to_hashable_key`, now via its real `Tuple` ARM rather than a
#     fall-through.
#
# So the boundary today is: NOTHING in this table raises. What distinguishes the
# columns is the CONTAINER — a UDT-bearing set is a `list` (#804), a map key is
# projected — and no shape depends on `value_to_py`'s output happening to be
# hashable.
# =============================================================================


def test_a_udt_inside_a_tuple_reads_as_the_804_list_of_tuples():
    """`set<frozen<tuple<frozen<udt>, int>>>` reads; the CONTAINER changed twice.

    Pre-#3504: `TypeError: unhashable type: 'dict'` — the tuple's UDT element was
    rendered by `value_to_py` as a `dict`. #3504 made it read, incidentally, as a
    `frozenset`, because `set_to_py`'s `contains_udt` did not look inside a tuple
    so the set took the hashing branch and the now-hashable `cqlite.Udt` fitted.

    #3500 then made `contains_udt` a full subtree traversal, so it DOES see the
    UDT under the tuple and `set_to_py` takes its #804 list-for-CLI-parity branch
    — a `list` of `(cqlite.Udt, int)` tuples. That is a DELIBERATE shape change
    (#3500 AC1 over AC5): it removes the nesting-dependent asymmetry whereby the
    same UDT-bearing set was a `list` at depth 1 and a `frozenset` at depth 2.

    Every CONTENT assertion below is unchanged by that move and is kept: the
    element is still an ordered pair of a `cqlite.Udt` and its position, still
    equal to an independently constructed value, and still hashable — a `list`
    container does not make its elements unhashable, so the equality-and-hash
    contract is asserted exactly as before.
    """
    stu = _shapes_row(1)["stu"]
    assert isinstance(stu, list), (
        f"expected #804's list branch (contains_udt sees the UDT under the "
        f"tuple since #3500), got {type(stu).__name__}"
    )
    assert not isinstance(stu, frozenset), (
        "a frozenset here means contains_udt stopped traversing the tuple — the "
        "#3500 revert signature"
    )
    assert len(stu) == 1

    element = stu[0]
    assert isinstance(element, tuple) and len(element) == 2
    udt, position = element
    assert position == 10
    assert isinstance(udt, cqlite.Udt)
    assert udt.type_name == "collide"
    assert udt.keyspace == "test_udt_collision"
    assert dict(udt.fields) == TUPLE_UDT_FIELDS

    # CONTENT, not merely "no exception": the element is recovered by an
    # INDEPENDENTLY CONSTRUCTED equal value. A test that only checked
    # `len(stu) == 1` would pass on a wrong-but-well-shaped projection.
    rebuilt = (cqlite.Udt("collide", "test_udt_collision", dict(TUPLE_UDT_FIELDS)), 10)
    assert rebuilt in stu
    # The hash contract is asserted even though the container no longer needs it:
    # the element remains usable in a hashed position, which is what
    # `map_to_py`'s KEY path (the `mtu` column below) actually depends on. Losing
    # it would break that path silently while this column stayed green.
    assert hash(rebuilt) == hash(element)


def test_a_udt_inside_a_tuple_now_projects_as_a_map_key():
    """`map<frozen<tuple<frozen<udt>, int>>, int>` projects; it used to raise.

    The same projection reached through `map_to_py`'s KEY conversion rather than
    through a set. Measured on `origin/main`: `TypeError: unhashable type:
    'dict'`.
    """
    mtu = _shapes_row(1)["mtu"]
    assert isinstance(mtu, dict) and len(mtu) == 1

    key, value = next(iter(mtu.items()))
    assert value == 5
    assert isinstance(key, tuple) and len(key) == 2
    udt, position = key
    assert position == 20
    assert isinstance(udt, cqlite.Udt)
    assert udt.type_name == "collide"
    assert dict(udt.fields) == TUPLE_UDT_FIELDS

    # Retrieval by a reconstructed key — the property a caller actually wants
    # from a dict, and the one that fails if the projected key's content differs
    # in any way from what is asserted above.
    rebuilt = (cqlite.Udt("collide", "test_udt_collision", dict(TUPLE_UDT_FIELDS)), 20)
    assert mtu[rebuilt] == 5

    # Type identity participates: the SAME field values under a different
    # declared type are a DIFFERENT key, so the tuple projection did not lose the
    # identity the rest of this change moved out of band.
    twin = (cqlite.Udt("collide_twin", "test_udt_collision", dict(TUPLE_UDT_FIELDS)), 20)
    assert twin not in mtu


def test_a_udt_bearing_set_in_a_hashed_position_now_reads():
    """`set<frozen<set<frozen<udt>>>>` NO LONGER RAISES — fixed by #3500.

    This was the OTHER side of the boundary and the one #3504 could not move,
    because its cause was never the UDT: the INNER set has a UDT element, so
    `set_to_py` returned a Python `list` for CLI parity (#804), and a `list` was
    unhashable in the OUTER set's `frozenset` branch. Measured identically before
    and after #3504: `TypeError: unhashable type: 'list'` — the error text naming
    `'list'` rather than `'dict'` is the evidence of which cause it was.

    #3500 removes it at the outer level: `contains_udt` now traverses the whole
    subtree, so the OUTER set also takes its #804 `list` branch and never asks
    for a hash. The column reads as a `list` of `list`s of `cqlite.Udt`s.

    This is #3504's OWN fixture reaching #3500's fix, i.e. an independent second
    fixture for that property (the first is
    `bindings/python/tests/test_nested_udt_hashable.py`'s `s_set_udt`). Kept as a
    test rather than deleted, because a revert of `contains_udt`'s `Set` arm would
    put the `TypeError` straight back and nothing else in THIS file would notice.
    """
    ssu = _shapes_row(2)["ssu"]
    assert isinstance(ssu, list), (
        f"expected #804's list branch at BOTH levels, got {type(ssu).__name__}"
    )
    assert len(ssu) == 1
    inner = ssu[0]
    assert isinstance(inner, list), (
        f"the INNER set is #804's list too, got {type(inner).__name__}"
    )
    assert len(inner) == 1

    # CONTENT: the UDT survived the container change intact, identity out of band.
    udt = inner[0]
    assert isinstance(udt, cqlite.Udt)
    assert udt.type_name == "collide"
    assert udt.keyspace == "test_udt_collision"
    assert dict(udt.fields) == TUPLE_UDT_FIELDS
    assert udt == cqlite.Udt("collide", "test_udt_collision", dict(TUPLE_UDT_FIELDS))


def test_a_full_scan_of_the_shapes_table_reads_every_row():
    """No shape in the table aborts a WHOLE-TABLE read any more.

    This case exists because a `TypeError` was raised while CONVERTING a row, so
    ONE unprojectable cell aborted the whole scan — which is the shape a caller
    actually writes, and which is why the other tests in this section read by
    primary key. #3500 made every column of this table projectable, so the scan
    completes; asserting the ROW COUNT rather than merely "no exception" keeps it
    from passing on an empty result.
    """
    with cqlite.open(FIXTURE_ROOT, schema=SCHEMA) as db:
        rows = list(db.execute(f"SELECT * FROM {SHAPES_TABLE}"))
    assert len(rows) == 3, f"expected the fixture's three rows, got {len(rows)}"
    # Non-vacuity: the row that used to abort the scan is present WITH its value.
    by_id = {row["id"]: row for row in rows}
    assert isinstance(by_id[2]["ssu"], list) and len(by_id[2]["ssu"]) == 1


def test_a_udt_with_a_collection_field_projects_with_that_field_as_a_dict():
    """The recorded decode gap is FIXED (#3631 instance B) — and the projection
    STILL succeeds, which is the part that had to be measured rather than
    predicted.

    HISTORY, kept because it is what this case was for: the field used to arrive
    as `bytes` (a collection field inside a frozen UDT decoded to `Value::Blob`),
    and this test pinned that as characterization, predicting that fixing the
    decode "may raise again" because `Udt.__hash__` hashes its field values and a
    `dict` is unhashable.

    RE-MEASURED after the fix, against the same committed corpus: `m` is now
    `{"a": 1}` and the projection SUCCEEDS anyway. The prediction was wrong for a
    reason that has nothing to do with the decode — #3500 made a UDT-bearing set
    render as a Python `list` (`contains_udt` traverses the whole subtree), and
    building a `list` never hashes its elements. So `Udt.__hash__` is not reached
    on this path at all, and no hashable projection of `m` is required.

    The `Udt.__hash__` residual is real all the same, and is STILL asserted on a
    HAND-BUILT value: the decoder now produces a `Udt` with a `dict` field, but no
    decoder path puts one where hashing is required. That is a statement about the
    CONTAINER, not about the field, which is why the hand-built assert stays.
    """
    stn = _shapes_row(3)["stn"]
    # `list`, not `frozenset`: the same #3500 container change as the `stu`
    # column above (contains_udt traverses the tuple). The GAP this test pins is
    # the FIELD's decode, which the container change does not touch.
    assert isinstance(stn, list) and len(stn) == 1
    udt, position = stn[0]
    assert position == 30
    assert udt.type_name == "unhashable_fields"
    assert udt.fields["label"] == "unhashable"
    # FIXED (#3631 instance B): the structured value, not the 17 serialized bytes.
    # Pinned by VALUE and not merely by type, because a `dict` of the wrong content
    # would satisfy an isinstance check while proving nothing about the decode.
    assert udt.fields["m"] == {"a": 1}, (
        f"expected the golden's {{'a': 1}}, got "
        f"{type(udt.fields['m']).__name__}={udt.fields['m']!r} — a `frozen<map<text,int>>` "
        "field of a frozen UDT must decode structurally (issue #3631 instance B)"
    )

    # The residual, at the only layer that can reach it today.
    with pytest.raises(TypeError, match=r"unhashable type: 'dict'"):
        hash(cqlite.Udt("t", "k", {"m": {"a": 1}}))
