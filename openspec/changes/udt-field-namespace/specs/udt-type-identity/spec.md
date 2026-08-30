# udt-type-identity Specification

## Purpose
A UDT's type identity SHALL be recoverable from a binding result independently of the UDT's own field
names, so that no field name can displace it. Issue #3504; adopted option (a).

## Requirements

### Requirement: NO field name displaces anything

Both bindings SHALL render a UDT such that its declared fields occupy a namespace that does NOT
contain the type name or the keyspace. A UDT declaring a field named `_type` and/or `_keyspace`
(legal CQL via a quoted identifier) SHALL yield **both** that field's value **and** the UDT's type
identity, each recoverable and neither overwritten. Neither binding SHALL place the type name or the
keyspace into the field namespace, and neither SHALL reject or rename a colliding field.

The requirement is over **every** field name, not an enumerated pair: a field name is DATA and SHALL
NOT be able to address any control channel of the host object model either. In particular the field
namespace SHALL NOT be an object that inherits accessors, since an ordinary property assignment on
such an object consults the prototype chain — so a field named `__proto__` would call an inherited
setter instead of becoming a field.

#### Scenario: Python — a UDT with both colliding field names round-trips

- **GIVEN** a `Value::Udt` for type `address` in keyspace `test_collections` whose fields are
  `_type = "user-supplied-type"`, `_keyspace = "user-supplied-ks"`, `street = "1 Main St"`
- **WHEN** it is converted to Python
- **THEN** the result is a `cqlite.Udt` whose `.type_name == "address"` and `.keyspace == "test_collections"`
- **AND** `.fields == {"_type": "user-supplied-type", "_keyspace": "user-supplied-ks", "street": "1 Main St"}`
- **AND** `udt["_type"] == "user-supplied-type"` (mapping access reaches the FIELD, never the marker)
- **AND** `set(udt.keys()) == {"_type", "_keyspace", "street"}` — the exact field-NAME SET, with no
  injected entries. Asserted as a SET rather than as a count: a count states only "three of
  something" and cannot see a field that was lost while an injected key took its place, which is the
  defect class this whole requirement is about.

#### Scenario: Node — the same UDT round-trips identically

- **GIVEN** the same `Value::Udt`
- **WHEN** it is converted to a JS value
- **THEN** the result satisfies `typeName === "address"` and `keyspace === "test_collections"`
- **AND** `fields` has exactly the three keys above with those values, asserted as a key SET
- **AND** `Object.keys(result)` contains no field name — the field namespace is `fields` alone.

#### Scenario: A non-colliding UDT keeps working through field access

- **GIVEN** a `Value::Udt` for `address` with fields `street`, `city` only
- **WHEN** converted in each binding
- **THEN** Python `udt["street"]`, `udt.fields["street"]`, `"city" in udt`, `dict(udt.fields)`,
  `sorted(udt.keys())` all resolve, and `.type_name`/`.keyspace` are populated
- **AND** Node `result.fields.street` resolves and `result.typeName` is populated
- **AND** no error is raised in either binding.

#### Scenario: Node — a field named `__proto__` is a field, not a prototype write

The same defect class as `_type`, one layer down: in JavaScript's own object model rather than in
ours. Measured on the Cassandra-written fixture BEFORE the fix — a string-valued `__proto__` field
vanished (absent from `Object.keys`, not an own property, reading back as `Object.prototype`), and a
null-valued one replaced the field bag's prototype with `null`.

- **GIVEN** a `Value::Udt` declaring a field named `__proto__` (legal CQL via a quoted identifier,
  exactly as `_type` is)
- **WHEN** it is converted to a JS value
- **THEN** `fields` holds `__proto__` as an own ENUMERABLE DATA property with the declared value, and
  `Object.keys(fields)` contains it
- **AND** the field bag's prototype is `null` for EVERY UDT — colliding or not, in value, key and
  element position — so the fix is a property of the CONSTRUCTION and not of the data, and no
  inherited accessor exists for any name to reach
- **AND** the fix SHALL NOT be a special case on the literal string `__proto__`: that is picking a
  rarer delimiter rather than removing the shared channel, and it would leave every other inherited
  name (including one a future JavaScript adds) able to intercept a declared field
- **AND** the outer object MAY keep a normal prototype, because its keys are chosen by the binding
  and never by data.

#### Scenario: Python — the same field name needs no special handling

- **GIVEN** the same UDT
- **WHEN** it is converted to Python
- **THEN** `__proto__` is an ordinary key of the `fields` mapping, reachable as `udt["__proto__"]`
- **AND** no analogous hazard exists to fix: `PyDict` insertion is a concrete dict store that consults
  no descriptor or inheritance chain, and Python keeps the mapping namespace (`udt[...]`) separate
  from the attribute namespace (`udt.type_name`), so a field name cannot reach a method or a property
  of `cqlite.Udt` either. Recorded explicitly rather than left unstated, since the two bindings must
  be shown to agree on SEMANTICS.

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

#### Scenario: Projection totality WIDENS, and the boundary is pinned

Making a UDT a hashable `cqlite.Udt` **did** change totality — an earlier draft of this scenario
claimed it did not, which was false. The new behaviour is kept (restoring a `TypeError` to preserve a
documented bug would be absurd); what is required is that the boundary be measured and pinned in both
directions. Measured on `test_udt_collision.udt_hashable_shapes`, with `origin/main`'s binding for the
"before" column.

- **GIVEN** a UDT reached through the arm-less `Tuple` fallthrough in a HASHED position —
  `set<frozen<tuple<frozen<udt>, int>>>` (a `frozenset` element) or
  `map<frozen<tuple<frozen<udt>, int>>, int>` (a `dict` key)
- **WHEN** the column is read through either binding surface
- **THEN** the projection SUCCEEDS, the projected key holds a `cqlite.Udt` with its declared fields,
  and the key is retrievable by an independently constructed equal value
- **AND** on `main` the identical input raised `TypeError: unhashable type: 'dict'`, because the
  fallthrough rendered the UDT as a `dict`.

- **GIVEN** a UDT-bearing `set` in a hashed position — `set<frozen<set<frozen<udt>>>>`
- **WHEN** the column is read
- **THEN** it STILL raises `TypeError: unhashable type: 'list'`, identically before and after, because
  `set_to_py` renders a UDT-bearing set as a Python `list` for CLI parity (#804) — a cause this change
  does not touch. The error naming `'list'` rather than `'dict'` is what identifies it.

- **AND** the `Tuple`/`Set` arms remain ABSENT from `value_to_hashable_key`: #3500 is not fixed, and
  the shapes above are resolved INCIDENTALLY by the UDT becoming hashable, not by adding an arm.
- **AND** `Udt.__hash__` SHALL still propagate `TypeError` for a genuinely unhashable field value.
  No decoder path reaches that today — a collection field inside a frozen UDT decodes to
  `Value::Blob`, i.e. hashable `bytes` — so it is asserted on a constructed value, and the decode gap
  is pinned as characterization.

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
