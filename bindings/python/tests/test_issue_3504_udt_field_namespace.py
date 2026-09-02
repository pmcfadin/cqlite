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
import signal
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

    # Every entry is DERIVED from this binding's own output; nothing here is a
    # literal. `cm`/`tm` (MULTICELL: the key lives in the CELL PATH) sit beside
    # `fcm`/`ftm` (FROZEN: a single value cell) because those are two different
    # decoders in cqlite-core and only the frozen one used to reach a UDT at all
    # (#3612). Carrying both makes this case a parity control in TWO directions
    # at once: cross-BINDING, as every entry here is, and cross-DECODE-PATH.
    observed = {
        "row1.c": _facts(rows[1]["c"]),
        "row1.p": _facts(rows[1]["p"]),
        "row1.cm_key": _facts(next(iter(rows[1]["cm"]))),
        "row1.tm_key": _facts(next(iter(rows[1]["tm"]))),
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

    # The map VALUES, one per map column, also derived from this binding.
    map_values = reference["map_values"]
    for column, fact_key in (
        ("cm", "row1.cm_value"),
        ("tm", "row1.tm_value"),
        ("fcm", "row1.fcm_value"),
        ("ftm", "row1.ftm_value"),
    ):
        assert next(iter(rows[1][column].values())) == map_values[fact_key], column
    # ...and those four values must be PAIRWISE DISTINCT in the reference, which
    # is what makes the four assertions above discriminating. The four map columns
    # hold the SAME key by construction, so a case that read the wrong column's
    # cell -- exactly the confusion a multicell/frozen pair invites -- would pass
    # unnoticed against equal values.
    declared = [
        map_values["row1.cm_value"],
        map_values["row1.tm_value"],
        map_values["row1.fcm_value"],
        map_values["row1.ftm_value"],
    ]
    assert len(set(declared)) == len(declared), declared

    # Non-vacuity: the reference must actually carry the colliding subject, or an
    # emptied/renamed file would let this pass having compared nothing.
    assert expected["row1.c"]["fields"]["_type"] == "user-supplied-type"
    assert expected["row1.c"]["typeName"] == "collide"
    # ...and the reference states the CROSS-DECODE-PATH identity in its own right:
    # the multicell key facts EQUAL the frozen ones, which is #3612's property (a
    # caller cannot tell the two spellings of one map apart). Stated here so the
    # committed FILE remains a valid control on its own -- the per-binding case
    # that measures this within one binding is
    # `test_non_frozen_map_udt_key_projects_like_the_frozen_control`, and this
    # case is what compares the two BINDINGS.
    assert expected["row1.cm_key"] == expected["row1.fcm_key"]
    assert expected["row1.tm_key"] == expected["row1.ftm_key"]


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


def test_a_udt_with_a_collection_field_projects_because_that_field_is_bytes():
    """RECORDED GAP + the residual: a `map`-typed UDT field decodes to `bytes`.

    The obvious prediction — a UDT declaring a `frozen<map<text,int>>` field
    stays unprojectable, because `Udt.__hash__` hashes its field values and a
    `dict` is unhashable — is FALSE here, and measurement is the only reason we
    know: CQLite decodes a collection field inside a frozen UDT as `Value::Blob`,
    so the field arrives as `bytes`, which IS hashable, and the projection
    succeeds. Recorded as CHARACTERIZATION, not as a desirable rendering: the
    correct value would be `{"a": 1}`. It is a decode-level gap, orthogonal to
    #3504, and pinned here so a future fix to it does not look like a regression
    in this file — it will red HERE, with this comment attached.

    The `Udt.__hash__` residual is real all the same, and is asserted on a
    HAND-BUILT value because no decoder path currently reaches it: a `Udt` whose
    field value genuinely is unhashable still propagates `TypeError`.
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
    # THE GAP: bytes, not {"a": 1}. Asserted as bytes so the pin is exact.
    assert isinstance(udt.fields["m"], bytes), (
        f"expected the recorded decode gap (bytes), got {type(udt.fields['m']).__name__} "
        "— if a collection field inside a frozen UDT now decodes properly, this "
        "projection may raise again; see the docstring"
    )

    # The residual, at the only layer that can reach it today.
    with pytest.raises(TypeError, match=r"unhashable type: 'dict'"):
        hash(cqlite.Udt("t", "k", {"m": {"a": 1}}))


# =============================================================================
# AC5 — the committed fixture resolves CHECKOUT-RELATIVE, never through
# CQLITE_DATASETS_ROOT (#3131/#3148; issue #3724 AC5)
# =============================================================================

# The child bound for the resolution probe below. MEASURED: unlike the Node side
# there is NO competing enforcer to stay under — `bindings/python/pyproject.toml`'s
# `[tool.pytest.ini_options]` sets no timeout and `pytest-timeout` is not
# installed (pytest 9.1.1) — so this bound is the ONLY bound, and it is kept
# generous (~150x the probe's measured warm runtime) rather than mirroring Node's
# tighter value, which exists there solely to stay below jest's 30s `testTimeout`.
_PROBE_TIMEOUT_SECS = 60


def _probe_stream(value: Any) -> str:
    """A probe capture stream as text, whatever shape the failure left it in.

    `TimeoutExpired` may carry `None`, and carries `bytes` when the run was not
    in text mode, so neither is assumed.
    """
    if value is None:
        return "(none)"
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return str(value)


def _probe_completion_failure(proc: Any, out_path: Path) -> str | None:
    """Why the probe did not complete, as a message naming the cause — or `None`.

    Mirrors the Node side's state set exactly (`probeCompletionFailure`), so the
    two bindings cannot report a different set of causes for the same probe. The
    two states that arrive as EXCEPTIONS rather than a result — a timeout and an
    unspawnable child — are named at the call site; the three visible in a
    completed `CompletedProcess` are named here.
    """
    detail = (
        f"--- child stdout ---\n{_probe_stream(proc.stdout)}\n"
        f"--- child stderr ---\n{_probe_stream(proc.stderr)}"
    )
    if proc.returncode < 0:
        # A negative returncode IS the signal number negated; `Signals` names it
        # when the platform knows it, and an unknown number is reported as-is
        # rather than guessed at.
        try:
            killed_by = signal.Signals(-proc.returncode).name
        except ValueError:
            killed_by = f"signal {-proc.returncode}"
        return (
            f"resolution probe was KILLED by {killed_by} "
            f"(bound {_PROBE_TIMEOUT_SECS}s)\n{detail}"
        )
    if proc.returncode != 0:
        return (
            f"resolution probe exited {proc.returncode} "
            f"(bound {_PROBE_TIMEOUT_SECS}s)\n{detail}"
        )
    if not out_path.is_file():
        return (
            f"resolution probe exited 0 but wrote no payload to {out_path} — nothing "
            f"was measured, so no path comparison below would mean anything\n{detail}"
        )
    return None


# The child-process probe for the behavioural half below. It re-evaluates THIS
# module — and `conftest` — from scratch in a fresh interpreter whose
# `CQLITE_DATASETS_ROOT` points at an empty directory, then records BOTH the
# paths this suite resolves AND a path that legitimately DOES follow that
# variable, so the parent can prove the perturbation was in effect.
_RESOLUTION_PROBE = r'''
import importlib.util
import json
import os
import sys
from pathlib import Path

tests_dir, module_path, out_path = sys.argv[1], sys.argv[2], sys.argv[3]
sys.path.insert(0, tests_dir)

import conftest  # noqa: E402  -- re-resolved under the perturbed environment
import cqlite  # noqa: E402

spec = importlib.util.spec_from_file_location("issue_3504_resolution_probe", module_path)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

payload = {
    # The POSITIVE CONTROL pair: what the child actually saw, and a constant
    # whose documented contract IS to follow it.
    "env_seen": os.environ.get("CQLITE_DATASETS_ROOT"),  # CONTROL: the probe echoes the perturbed value back
    # CONTROL: env-routed BY CONTRACT — the positive control, never a consumer.
    "control_env_routed": str(conftest.DATASETS),  # CONTROL
    # The INVARIANT: this suite's own resolved constants. `SCHEMAS`/`SCHEMA` are
    # deliberately NOT recorded: AC5 is about the DATASETS root, and the schemas
    # root is a separate contract that the Node side legitimately lets
    # `CQLITE_SCHEMAS_ROOT` relocate — pinning it here would certify Python's
    # non-honouring as correct and would red, with a fixture-resolution message,
    # the day `conftest` gains the #3148 override.
    "fixture_root": str(module.FIXTURE_ROOT),
    "parity_facts": str(module.PARITY_FACTS),
    "row_ids": None,
    "read_error": None,
    "parity_facts_udts": None,
    "parity_facts_error": None,
}

# AC5 covers the parity-facts FILE as much as the corpus, so the probe OPENS it
# rather than only recording its path — otherwise that half is path-equality
# only while the corpus half is path-equality PLUS a read-back.
try:
    with open(module.PARITY_FACTS, encoding="utf-8") as handle:
        payload["parity_facts_udts"] = len(json.load(handle)["udts"])
except Exception as exc:  # noqa: BLE001 -- reported to the parent, not handled
    payload["parity_facts_error"] = f"{type(exc).__name__}: {exc}"

# The read is attempted AFTER the paths are recorded, and its failure is
# REPORTED rather than raised: an env-routed resolution makes the open fail, and
# the parent must be able to name the path mismatch that caused it instead of
# reporting only a dead child.
try:
    with cqlite.open(module.FIXTURE_ROOT, schema=module.SCHEMA) as db:
        payload["row_ids"] = sorted(row.get("id") for row in db.execute(module.QUERY).rows)
except Exception as exc:  # noqa: BLE001 -- reported to the parent, not handled
    payload["read_error"] = f"{type(exc).__name__}: {exc}"

Path(out_path).write_text(json.dumps(payload))
'''


def test_fixture_and_parity_facts_resolve_checkout_relative_not_via_the_env(tmp_path):
    """SCENARIO: the committed fixture paths do not hang off `CQLITE_DATASETS_ROOT`.

    The module docstring above and the reference file's `note_on_paths` DOCUMENT
    this contract; nothing ASSERTED it. `_assert_fixture_present` cannot: it
    checks the file exists at the ALREADY-RESOLVED path, so it would pass
    unchanged if resolution became env-routed and the env root happened to hold
    the file. Committed fixtures are committed SOURCE and resolve
    checkout-relative — and the corpus resolvers are an EITHER/OR on the
    variable (`conftest.py:42-48`), so a fixture reached through one is invisible
    exactly where the suite runs, because every gate run sets it.

    Two halves, and the second is the one that catches a regression.

    AFFIRMATIVE EQUALITY. The resolved paths must EQUAL the checkout-derived
    expectation. A "the env value is not a prefix" check would go vacuous
    whenever the variable is unset or coincidentally equals the checkout — a
    pass derived from the absence of a bad signal, which CLAUDE.md forbids. The
    expectation is derived UNCANONICALIZED, matching how `conftest` derives
    `PROJECT_ROOT` (`Path(__file__).parent` chains, no `resolve()`): canonicalizing
    one side only would red on a correct checkout reached through a symlink, and
    a guard that reds on correct input is the guard agents learn to waive. The
    canonicalized forms are compared too — like with like on both sides.

    BEHAVIOURAL INVARIANCE, MEASURED IN A CHILD PROCESS. Module-level constants
    freeze at import, so an in-process reload of a neighbouring module cannot
    observe a resolution that reads the variable DIRECTLY at load time: that
    check stays green whenever the variable was unset when this module was first
    imported. So the probe re-evaluates THIS module in a fresh interpreter whose
    `CQLITE_DATASETS_ROOT` is an empty directory, and asserts a PAIR:

      * the POSITIVE CONTROL — the child echoes back the perturbed value, and
        `conftest.DATASETS`, whose documented contract IS to follow that
        variable, HAS moved onto it. Without this the invariant below would be
        satisfiable by an environment the child never saw.
      * the INVARIANT — the three paths are unmoved and checkout-derived, and
        the fixture still reads its three rows through them.

    The parent mutates no environment variable and no module state, so nothing
    can leak to a sibling test.

    NOT ASSERTED: `CQLITE_SCHEMAS_ROOT`. Python's `SCHEMAS` is purely
    checkout-derived and is pinned below, but the Node side legitimately honours
    that variable (`setup.js:67-102` — the gate-validated #3148 contract), so
    honouring it is not an AC5 violation in either binding.
    """
    import os
    import subprocess
    import sys

    # FIRST, and before anything can be misattributed: a checkout missing the
    # committed fixture must fail as a BROKEN CHECKOUT, naming the absent
    # artifact. Without this the probe's read failure below would be reported
    # under a corpus-less-root heading it has not established — the Node case
    # gets this from the `beforeAll` that its `describe` runs.
    _assert_fixture_present()

    # UNCANONICALIZED, to match `conftest.PROJECT_ROOT`'s own derivation.
    repo_root = Path(__file__).parents[3]
    expected_root = repo_root / "test-data" / "fixtures" / "issue_3504"
    expected_facts = expected_root / "binding-parity-facts.json"

    # The AMBIENT value, recorded in every failure below. Half 1's discriminating
    # power depends on it: SET (as every gate run has it) and an env-routed
    # resolution reds on the equality alone; UNSET (the usual local run) and only
    # Half 2 can see it. A maintainer reading a failure needs to know which run
    # they are looking at, and neither state is the "right" one to run under.
    ambient = os.environ.get("CQLITE_DATASETS_ROOT")  # CONTROL: the AMBIENT read, diagnostic only
    ambient_note = (
        "ambient CQLITE_DATASETS_ROOT: "
        f"{ambient if ambient is not None else '(unset)'}"
    )

    # Half 1 — the resolved constants ARE the checkout-derived paths. SCHEMAS and
    # SCHEMA are deliberately NOT pinned here (see the probe payload comment).
    assert FIXTURE_ROOT == expected_root, ambient_note
    assert PARITY_FACTS == expected_facts, ambient_note
    # ...and equal canonicalized too, both sides canonicalized.
    assert FIXTURE_ROOT.resolve() == expected_root.resolve(), ambient_note
    assert PARITY_FACTS.resolve() == expected_facts.resolve(), ambient_note

    # Half 2 — a fresh interpreter under a datasets root holding no corpus.
    bogus = tmp_path / "no-corpus-here"
    bogus.mkdir()
    assert not sorted(bogus.iterdir())
    out_path = tmp_path / "probe.json"

    child_env = dict(os.environ)
    child_env["CQLITE_DATASETS_ROOT"] = str(bogus)  # CONTROL: the perturbation itself
    # The strict-fixture flags are cleared for the CHILD only: a corpus-less
    # root makes them a hard failure by design, which would leave the probe
    # unable to run rather than able to measure.
    child_env.pop("CQLITE_REQUIRE_FIXTURES", None)
    child_env.pop("CQLITE_PARITY_REQUIRE_DATASETS", None)

    # AFFIRMATIVE COMPLETION ASSERTS, before a single byte of the payload is
    # read. A timed-out or dead probe must fail NAMING that, and must never fall
    # through into comparing absent output against an expected path: that either
    # misleads (a "path mismatch" for a hang) or, with no payload written at all,
    # risks a comparison that passes having measured nothing.
    try:
        proc = subprocess.run(
            [
                sys.executable,
                "-c",
                _RESOLUTION_PROBE,
                str(Path(__file__).parent),
                str(Path(__file__)),
                str(out_path),
            ],
            env=child_env,
            capture_output=True,
            text=True,
            timeout=_PROBE_TIMEOUT_SECS,
        )
    except subprocess.TimeoutExpired as expired:
        pytest.fail(
            f"resolution probe TIMED OUT after {_PROBE_TIMEOUT_SECS}s. It normally "
            "completes in well under a second, so this is a hang, not a slow box — "
            "nothing about path resolution was measured.\n"
            f"{ambient_note}\n"
            f"--- child stdout ---\n{_probe_stream(expired.stdout)}\n"
            f"--- child stderr ---\n{_probe_stream(expired.stderr)}",
            pytrace=False,
        )
    except OSError as spawn_error:
        # The unspawnable state, which used to surface as a raw traceback.
        pytest.fail(
            "resolution probe could not be spawned: "
            f"{spawn_error.__class__.__name__}: {spawn_error} "
            f"(bound {_PROBE_TIMEOUT_SECS}s)\n{ambient_note}",
            pytrace=False,
        )
    failure = _probe_completion_failure(proc, out_path)
    assert failure is None, f"{failure}\n{ambient_note}"
    payload = json.loads(out_path.read_text())

    # POSITIVE CONTROL — the perturbation really was in effect, and a constant
    # whose contract is to follow the variable really did move onto it.
    assert payload["env_seen"] == str(bogus), (
        "the probe did not see the perturbed CQLITE_DATASETS_ROOT — the "
        f"invariance below would prove nothing\n{ambient_note}"
    )
    assert payload["control_env_routed"] == str(bogus), (
        "conftest.DATASETS did not follow the perturbed CQLITE_DATASETS_ROOT — "
        f"got {payload['control_env_routed']}; the control is broken, so the "
        f"invariance below is unmeasured\n{ambient_note}"
    )
    assert payload["control_env_routed"] != str(expected_root)

    # THE INVARIANT — unmoved, checkout-derived, and both artifacts still read.
    assert payload["fixture_root"] == str(expected_root), ambient_note
    assert payload["parity_facts"] == str(expected_facts), ambient_note

    # A read failure is REPORTED without a cause being claimed. `_assert_fixture_present`
    # above has already ruled out the broken-checkout case, but a decoder
    # regression, a schema-parse change or a force-added binary lost to a
    # gitignore would all look identical here, and a message naming one of them
    # would be asserting what this test has not established.
    assert payload["read_error"] is None, (
        "the probe could not read the fixture through the re-resolved paths "
        f"(cause NOT established by this test)\n{ambient_note}\n"
        f"child error: {payload['read_error']}"
    )
    assert payload["row_ids"] == [1, 2, 3], (
        f"unexpected fixture row ids through the re-resolved paths: "
        f"{payload['row_ids']}\n{ambient_note}"
    )
    assert payload["parity_facts_error"] is None, (
        "the probe could not read the parity reference through the re-resolved "
        f"path (cause NOT established by this test)\n{ambient_note}\n"
        f"child error: {payload['parity_facts_error']}"
    )
    # Non-vacuity: an emptied or renamed reference would otherwise let the
    # path-equality half stand in for a file nobody opened. Asserted NON-ZERO
    # rather than at an exact count, which is #3724's own subject to widen.
    assert payload["parity_facts_udts"] > 0, (
        "the parity reference parsed but carries no `udts` entries — the path "
        f"equality above would then be comparing a path to an empty file\n{ambient_note}"
    )


def test_this_module_names_no_env_routed_corpus_constant():
    """SCENARIO: nothing in this file builds a path from an env-routed corpus root.

    THE CLASS THIS CLOSES, WHICH THE ENVIRONMENT CASES ONLY SAMPLE. The test
    above pins the CONSTANTS this module resolves today. A future test added to
    this file that builds its OWN path from `conftest`'s env-routed corpus
    constant is invisible to it: that path would resolve through
    `CQLITE_DATASETS_ROOT` while every assertion above stayed green, because
    those assertions are about `FIXTURE_ROOT`/`PARITY_FACTS` and not about the
    file's other consumers. This repository has ALREADY paid for exactly that
    defect one binding over — `bindings/node/__test__/setup.js`'s round-10 note
    records `write.test.js` and `write-smoke.test.js` building the schemas path
    themselves and BYPASSING the resolver, so the variable was honoured by part
    of the suite and ignored by the rest.

    Answered from THIS FILE'S OWN SOURCE, TOKENIZED. Only `NAME` tokens count, so
    a mention in a docstring or a comment is not a consumer — which matters here,
    because this file discusses the env-routed constant at length in prose. The
    needles are SPLIT so the scan cannot match its own source; with an
    exact-token comparison a self-match is already structurally impossible (a
    literal in this function is a `STRING` token, never a `NAME`), so the split is
    belt-and-braces rather than the mechanism.

    A SECOND NEEDLE, closing the cheap half of what this guard used to merely
    declare (roborev #3724 round 4): the env VARIABLE NAME itself. A future test
    that skips the corpus constants and reads `os.environ["<var>"]` DIRECTLY names
    none of the constants above, so the NAME scan cannot see it — but such a read
    must contain the variable's name as a string literal, and a literal in this
    file's source plainly IS checkable.

    Compared by EVALUATED VALUE, not by source spelling (roborev #3724 round 5).
    Matching two quoted spellings was evadable by writing the same literal
    differently, and MEASURED on this interpreter (3.12) each of these parses to a
    single `ast.Constant` whose value is exactly the variable name, so all of them
    are caught by ONE comparison:
    `"X"`, `'X'`, `r"X"`, `\"\"\"X\"\"\"`, `f"X"` (no interpolation), and the
    implicit adjacent-literal concatenation `"CQLITE_" "DATASETS_ROOT"`.
    Reading the VALUE also SHRINKS this check rather than growing it: one
    comparison replaces a set of accepted spellings, and the exemptions do not
    have to grow to accommodate prose, because a docstring's `Constant` holds the
    ENTIRE docstring and a message like `"ambient <var>: "` is a different string
    again — neither is ever equal to the bare name. Comments are not in the AST at
    all. This is `ast` used as the language's own literal reader, NOT dataflow or
    reachability analysis: nothing here asks whether a read is executable.

    WHAT THE TWO GUARDS COVER, AND WHAT NOTHING HERE COVERS. This paragraph used to
    say the residual "stays the child-process probe's job — whatever route a
    consumer took to read it". That was FALSE, and correcting it is the point of
    this note (roborev N-C1): the probe records and compares only the constants
    this module EXPORTS — it reads `module.FIXTURE_ROOT`, `module.PARITY_FACTS`,
    `module.SCHEMA` and `module.QUERY` back out of a freshly imported copy of this
    file — so it cannot observe a path that some other test builds inside its own
    body. Stated as it actually is:

    * THIS SCAN catches the env variable's name written as a LITERAL, in any
      spelling (compared by evaluated VALUE, so quoting is irrelevant), plus any of
      the named corpus constants.
    * THE CHILD PROBE above proves those EXPORTED CONSTANTS stay checkout-anchored
      while `CQLITE_DATASETS_ROOT` is perturbed.
    * NEITHER covers an INDIRECT read — a helper that returns the value, a COMPUTED
      or concatenated name (`os.environ[prefix + suffix]`, or an INTERPOLATING
      f-string, which parses to `Constant("CQLITE_")` + `Constant("DATASETS_ROOT")`,
      neither equal to the whole — MEASURED), or an alias bound to `os.environ` —
      used by a FUTURE TEST IN THIS FILE to build its OWN path. That is an
      UNCOVERED RESIDUAL, not a covered one: the read names neither a corpus
      constant nor the literal, so this scan is blind, and such a path never
      reaches an exported constant, so the probe is blind to it as well.

    Deliberately NOT closed by widening either guard. These two already exceed what
    AC5 asks for, and a recogniser over computed names is the unbounded shape this
    repository keeps having to delete — it accumulates false PASSes and an exemption
    list that grows every round, and a guard with known false PASSes is worse than
    no guard. A narrow guard that says what it does NOT cover is worth more than a
    broad one implying completeness it cannot deliver.
    """
    import ast
    import io
    import tokenize

    # Split, per the note above.
    forbidden = {
        "DATA" + "SETS",
        "SSTABLES" + "_DIR",
        "TEST_DATA" + "_ROOT",
        "DATASETS" + "_AVAILABLE",
    }

    # Split for the same reason, and here the split is load-bearing in a second
    # way: an unsplit literal below would itself be a `Constant` equal to the
    # needle, so the scan would match its own source and could never pass. The
    # EXPLICIT `+` is what makes the split work — MEASURED: `ast` does NOT fold a
    # `BinOp`, so this stays two Constants, while an IMPLICIT adjacent-literal
    # concatenation WOULD be folded into one and would self-match.
    env_var = "CQLITE_" + "DATASETS_ROOT"

    source_text = Path(__file__).read_text()
    source_lines = source_text.splitlines()
    tokens = list(tokenize.tokenize(io.BytesIO(Path(__file__).read_bytes()).readline))

    names = {token.string for token in tokens if token.type == tokenize.NAME}
    offenders = sorted(forbidden & names)
    assert not offenders, (
        f"this module names the env-routed corpus constant(s) {offenders} in CODE. "
        "The committed fixture is committed SOURCE and resolves checkout-relative "
        "from `PROJECT_ROOT` (#3131/#3148); a path built from an env-routed root "
        "is invisible exactly where this suite runs, because every gate run sets "
        "CQLITE_DATASETS_ROOT. Build the path from `PROJECT_ROOT` instead."
    )

    # Needle 2 — the env variable's own name as a string literal VALUE, on any
    # line that does not declare itself the positive CONTROL. Line numbers come
    # from the AST nodes, so the diagnostic still names the offending line.
    env_literal_lines = [
        node.lineno
        for node in ast.walk(ast.parse(source_text))
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and node.value == env_var
    ]
    env_offenders = sorted(
        f"{lineno}: {source_lines[lineno - 1].strip()}"
        for lineno in env_literal_lines
        if "CONTROL" not in source_lines[lineno - 1]
    )
    assert not env_offenders, (
        f"this module reads the {env_var} environment variable directly, outside "
        f"its positive control: {env_offenders}. The committed fixture is committed "
        "SOURCE and resolves checkout-relative from `PROJECT_ROOT` (#3131/#3148); a "
        "path built from an env-routed root is invisible exactly where this suite "
        "runs, because every gate run sets that variable. Build the path from "
        "`PROJECT_ROOT` instead — or, if the reference really is a positive "
        "control, mark the line CONTROL and say why."
    )

    # NON-VACUITY for needle 2, counted SEPARATELY from needle 1's: folding the two
    # totals into one would let a typo in either split hide behind the other's
    # matches. Counted over the AST nodes the needle actually examines, so MEASURED
    # at 2 -- the ambient diagnostic read and the perturbation. The probe's own
    # echo is a THIRD reference, invisible here because it lives inside one big
    # string literal whose VALUE is the whole probe, and it is checked as PROBE
    # SOURCE below instead.
    env_control_lines = [
        source_lines[lineno - 1]
        for lineno in env_literal_lines
        if "CONTROL" in source_lines[lineno - 1]
    ]
    assert len(env_control_lines) == 2, (
        f"expected exactly 2 CONTROL-marked references to {env_var} in this "
        f"module's code, got {len(env_control_lines)}: {env_control_lines}. A count "
        "that drifts means either a new consumer wearing the marker, or a split "
        "needle typo that now matches nothing."
    )

    # The probe's SOURCE is a string literal, so the token scan above cannot see
    # it — and it legitimately names the env-routed constant, ONCE, as the
    # positive control. That one line is therefore required to SAY it is the
    # control, so the exemption cannot silently grow into a consumer.
    control_needle = "conftest." + "DATA" + "SETS"
    control_lines = [
        line for line in _RESOLUTION_PROBE.splitlines() if control_needle in line
    ]
    assert len(control_lines) == 1, (
        f"expected exactly one control reference to {control_needle} in the probe "
        f"source, got {len(control_lines)}: {control_lines}"
    )
    assert "CONTROL" in control_lines[0], (
        "the probe's reference to the env-routed constant must declare itself the "
        f"positive CONTROL, or it is indistinguishable from a consumer: {control_lines[0]!r}"
    )

    # ...and the same discipline for the probe's reference to the env VARIABLE. The
    # probe legitimately reads it, ONCE, to echo the perturbed value back as the
    # positive control's proof; a second reference would be a consumer.
    # Substring, not a spelling set: the probe source is a string to this module,
    # so there is no AST of it to read values from — and a substring test is
    # spelling-insensitive for the same reason the Node guard's is.
    probe_env_lines = [
        line for line in _RESOLUTION_PROBE.splitlines() if env_var in line
    ]
    assert len(probe_env_lines) == 1, (
        f"expected exactly one reference to {env_var} in the probe source, got "
        f"{len(probe_env_lines)}: {probe_env_lines}"
    )
    assert "CONTROL" in probe_env_lines[0], (
        "the probe's read of the env variable must declare itself the positive "
        f"CONTROL, or it is indistinguishable from a consumer: {probe_env_lines[0]!r}"
    )
