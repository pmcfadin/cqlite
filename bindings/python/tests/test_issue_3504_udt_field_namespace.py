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

WHY THIS FILE RESOLVES ITS OWN PATHS. `conftest.DATASETS` comes from
`CQLITE_DATASETS_ROOT` and never falls back to the checkout, so a fixture reached
through it would be INVISIBLE on any box with that variable set — i.e. on every
gate run. The fixture is committed checkout-relative and is resolved from
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

    # ...and the declared fields, all three, unmodified.
    assert dict(udt.fields) == {
        "_type": "user-supplied-type",
        "_keyspace": "user-supplied-keyspace",
        "real_field": 42,
    }
    # The field count, with NO injected entries. Under the old shape this was 3
    # as well (two injected keys, both overwritten) — which is why the values,
    # not just the count, are asserted above.
    assert len(udt) == 3

    # Mapping access reaches the FIELD, never the marker.
    assert udt["_type"] == "user-supplied-type"
    assert udt["_keyspace"] == "user-supplied-keyspace"
    assert udt["real_field"] == 42
    assert "_type" in udt and "_keyspace" in udt
    assert sorted(udt.keys()) == ["_keyspace", "_type", "real_field"]
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
        "real_field": 0,
    }
    assert len(udt) == 3


# =============================================================================
# Site 4 — the hashable projection (`value_to_hashable_key`)
# =============================================================================


def test_projected_map_key_holds_exactly_one_type_entry(rows):
    """SCENARIO: a field named `_type` no longer yields a duplicate pair.

    The subject is `fcm` — `frozen<map<frozen<collide>, int>>` — and NOT the
    non-frozen `cm`. Measured on this fixture: a non-frozen map is multicell, so
    its key lives in the cell path, which decodes to `Value::Blob` for a frozen
    UDT and never reaches the projection at all (pinned below).

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
    assert len(names) == 3
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
        "real_field": 200,
    }


def test_non_frozen_map_udt_key_never_reaches_the_projection(rows):
    """RECORDED GAP (decode-level, out of #3504's scope): `cm`/`tm` keys are blobs.

    A NON-frozen `map<frozen<udt>, int>` is multicell, so its key lives in the
    CELL PATH, and `parse_cell_path_key`
    (`cqlite-core/src/storage/sstable/reader/parsing/row_decoder/complex_column.rs`)
    matches a closed set of PRIMITIVE cell-path types and falls back to
    `Value::Blob` for a frozen UDT. So such a key decodes to `bytes` and never
    reaches the UDT projection.

    Pinned here as characterization, not as a desirable shape: it is the spelling
    a user would most naturally write, and without this assertion a future reader
    would reasonably assume `cm` covers site 4 — it does not, which is why the
    fixture also carries the frozen `fcm`/`ftm`. Details:
    `test-data/fixtures/issue_3504/README.md`.
    """
    for column in ("cm", "tm"):
        cell = rows[1][column]
        assert isinstance(cell, dict) and len(cell) == 1, column
        key = next(iter(cell))
        assert isinstance(key, bytes), (
            f"{column}: expected the documented Blob cell-path key, "
            f"got {type(key).__name__} — if this now decodes to a UDT the "
            "recorded gap has closed and this test should become a positive assertion"
        )


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
