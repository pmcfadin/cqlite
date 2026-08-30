# udt-type-identity Specification

## Purpose
A UDT's type identity SHALL be recoverable from a binding result independently of the UDT's own field
names, so that no field name can displace it. Issue #3504; adopted option (a).

## Requirements

### Requirement: A UDT field named `_type` or `_keyspace` displaces nothing

Both bindings SHALL render a UDT such that its declared fields occupy a namespace that does NOT
contain the type name or the keyspace. A UDT declaring a field named `_type` and/or `_keyspace`
(legal CQL via a quoted identifier) SHALL yield **both** that field's value **and** the UDT's type
identity, each recoverable and neither overwritten. Neither binding SHALL place the type name or the
keyspace into the field namespace, and neither SHALL reject or rename a colliding field.

#### Scenario: Python — a UDT with both colliding field names round-trips

- **GIVEN** a `Value::Udt` for type `address` in keyspace `test_collections` whose fields are
  `_type = "user-supplied-type"`, `_keyspace = "user-supplied-ks"`, `street = "1 Main St"`
- **WHEN** it is converted to Python
- **THEN** the result is a `cqlite.Udt` whose `.type_name == "address"` and `.keyspace == "test_collections"`
- **AND** `.fields == {"_type": "user-supplied-type", "_keyspace": "user-supplied-ks", "street": "1 Main St"}`
- **AND** `udt["_type"] == "user-supplied-type"` (mapping access reaches the FIELD, never the marker)
- **AND** `len(udt) == 3` — the field count, with no injected entries.

#### Scenario: Node — the same UDT round-trips identically

- **GIVEN** the same `Value::Udt`
- **WHEN** it is converted to a JS value
- **THEN** the result satisfies `typeName === "address"` and `keyspace === "test_collections"`
- **AND** `fields` has exactly the three keys above with those values
- **AND** `Object.keys(result)` contains no field name — the field namespace is `fields` alone.

#### Scenario: A non-colliding UDT keeps working through field access

- **GIVEN** a `Value::Udt` for `address` with fields `street`, `city` only
- **WHEN** converted in each binding
- **THEN** Python `udt["street"]`, `udt.fields["street"]`, `"city" in udt`, `dict(udt.fields)`,
  `sorted(udt.keys())` all resolve, and `.type_name`/`.keyspace` are populated
- **AND** Node `result.fields.street` resolves and `result.typeName` is populated
- **AND** no error is raised in either binding.

#### Scenario: The marker is no longer readable from the field namespace

- **GIVEN** the non-colliding UDT above
- **WHEN** Python evaluates `udt["_type"]` and Node evaluates `result._type`
- **THEN** Python raises `KeyError` and Node yields `undefined`
- **AND** the type name remains available as `udt.type_name` / `result.typeName`.
  (This is the removed shared channel, asserted as removed — not an incidental regression.)

### Requirement: Cross-binding parity of UDT semantics

The two bindings SHALL expose the same UDT semantics, differing only in language-conventional
spelling (`type_name`/`typeName`). The set of recoverable facts — type name, keyspace, the exact field
mapping — SHALL be equal for the same input, and a change to one binding's UDT shape SHALL NOT land
without the corresponding change to the other.

#### Scenario: The same UDT yields equal facts in both bindings

- **GIVEN** the colliding UDT from the first scenario
- **WHEN** rendered through Python and through Node
- **THEN** the type name, the keyspace, and the field-name→value mapping are pairwise equal across the
  two bindings, compared as data rather than by host type.

### Requirement: The hashable projection carries identity out of band and emits no duplicate pair

`value_to_hashable_key`'s `Udt` arm (Python only — Node keys a real JS `Map` by the object and needs no
projection) SHALL project a UDT to a value that carries the type name and keyspace **outside** the
field-pair set. The projection SHALL contain exactly one entry per declared field and no entry for the
type name or keyspace. Two UDTs of different declared types with identical fields SHALL project to
unequal, distinctly-hashing values.

#### Scenario: A field named `_type` no longer yields a duplicate pair

- **GIVEN** a `map<frozen<address>, int>` whose key UDT declares a field `_type`
- **WHEN** the map is converted to a Python `dict`
- **THEN** the projected key exposes exactly one `_type` entry — the field's — and the UDT's type name
  is recoverable from the projected key without reading the field namespace
- **AND** on `main` the same input yields two `_type` pairs in the projected `frozenset`.

#### Scenario: Same fields, different UDT types stay distinct as map keys

- **GIVEN** two UDTs `a.point` and `b.point`, each with identical fields `{x: 1, y: 2}`
- **WHEN** both are used as keys of one Python `dict` built by `map_to_py`
- **THEN** the dict has two distinct entries (the projections are unequal and hash differently).

#### Scenario: Projection totality is unchanged

- **WHEN** the projection is exercised over UDT shapes that currently succeed
- **THEN** it still succeeds
- **AND** the `Tuple`/`Set` arms remain absent — the `TypeError` on nested UDTs in set elements / map
  keys is #3500 and is neither fixed nor worsened here.

### Requirement: Declared stubs match the runtime surface

The Python `.pyi` stub and the Node `index.d.ts` SHALL declare the new UDT surface exactly as the
runtime exposes it, and SHALL NOT continue to declare the removed flat shape. The #1456 stub-fidelity
drift alarms SHALL pass without widening any exemption set — in particular
`TYPE_ONLY_STUB_NAMES` SHALL remain empty and `index.d.ts` SHALL remain free of `any`.

#### Scenario: Python stub declares `cqlite.Udt`

- **WHEN** `bindings/python/tests/test_stub_fidelity.py` runs
- **THEN** it passes with `cqlite.Udt` declared in `__init__.pyi` with its members, matching
  `vars(cqlite)` in both directions
- **AND** `TYPE_ONLY_STUB_NAMES` is still empty.

#### Scenario: Node `UdtValue` no longer declares an index signature

- **WHEN** `bindings/node/__test__/typescript-definitions.test.js` runs
- **THEN** `interface UdtValue` declares `typeName: string`, `keyspace: string` and a `fields` mapping
- **AND** it declares NO `[field: string]: Value` index signature (that signature is what permitted the
  collision)
- **AND** the runtime-surface drift alarm and the no-`any` assertion both pass.

### Requirement: The documented defect class narrows to what survives

`docs/development/M4_spec.md` §5.3 SHALL be updated so the `_type`/`_keyspace` marker class (instance
`b-2`) records sites 3 and 4 as **FIXED** with the mechanism, retains the still-open sites without
claiming more than is true, and does not leave a superseded shape described as current. The oracle
table SHALL be corrected where this change alters which surfaces inject a marker.

#### Scenario: §5.3 matches the shipped behaviour

- **WHEN** §5.3 is read after the change
- **THEN** the UDT-fields and UDT-as-map-key-projection rows read FIXED, naming the out-of-band shape
- **AND** the cell-level-map row remains OPEN, attributed to #3497, noting the structural signal this
  change makes available
- **AND** no example in §5.3 (or in either binding README) still shows `{_type, _keyspace, **fields}` as
  the current shape.
