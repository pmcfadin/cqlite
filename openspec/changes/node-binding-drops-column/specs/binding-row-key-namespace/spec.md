# binding-row-key-namespace Specification

## Purpose
A column name, and a JSON object key, are **DATA**. Neither SHALL be able to address any control
channel of the host language's object model, so no declared column can be silently lost or silently
change the object model of a value the binding returns. Issue #3630; the row and JSON-object halves
of the class #3504 closed for the UDT field bag.

## ADDED Requirements

### Requirement: A row column is defined, never assigned

The Node binding SHALL construct a row such that every emitted column becomes an **own enumerable
writable configurable data property**, created by a mechanism that does **not** perform a JavaScript
`[[Set]]` and therefore never consults the prototype chain. A row SHALL retain
`Object.prototype` as its prototype.

The requirement is over **every** column name, not an enumerated set: the fix SHALL NOT special-case
the literal string `__proto__`, or any literal name. A name-literal check is picking a rarer
delimiter rather than removing the shared channel, and would leave every other inherited name —
including any a future JavaScript adds to `Object.prototype` — able to intercept a declared column.

It applies identically to **both** write paths in `row_to_object`: the interned-key path (a column in
`metadata.columns`) and the extras path (a value the authoritative column list does not cover). A
change reaching only one leaves the other live.

#### Scenario: A string-valued column named `__proto__` arrives as a column

- **GIVEN** a Cassandra-written table declaring a quoted column `"__proto__"` and a row whose
  `"__proto__"` holds the string `user-supplied-proto`
- **WHEN** the row is read through the Node binding's public query surface
- **THEN** `Object.hasOwn(row, '__proto__')` is `true` and `row['__proto__'] === 'user-supplied-proto'`
- **AND** `Object.keys(row)` **contains** `'__proto__'`
- **AND** `Object.getOwnPropertyDescriptor(row, '__proto__')` reports
  `{ value: 'user-supplied-proto', writable: true, enumerable: true, configurable: true }` — a data
  property, not an accessor
- **AND** `Object.entries(row)` contains the pair, and `JSON.parse(JSON.stringify(row))['__proto__']`
  is the same string
- **AND** the key set is asserted as a **SET**, never as a count: a count states only "N of
  something" and cannot see a column that was lost, which is the defect class this requirement is
  about.

#### Scenario: A null-valued column named `__proto__` does not change the row's prototype

- **GIVEN** the same table and a row whose `"__proto__"` is NULL
- **WHEN** the row is read through the Node binding
- **THEN** `Object.getPrototypeOf(row) === Object.prototype` — the same prototype as a row with no
  such column, before and after
- **AND** `Object.hasOwn(row, '__proto__')` is `true` and `row['__proto__']` is `null`
- **AND** `Object.keys(row)` contains `'__proto__'`.

#### Scenario: The same two cases hold on the EXTRAS path

- **GIVEN** a result whose authoritative column list does **not** cover a value keyed `__proto__`
  (an aggregate value keyed differently from its metadata name, or a result whose schema lookup left
  `metadata.columns` empty while rows are still yielded)
- **WHEN** the row is read through the Node binding
- **THEN** both scenarios above hold for that value, by the same mechanism
- **AND** the test reaching this path asserts it **reached** it (the value is absent from
  `result.columns`), so a case that silently took the interned path instead cannot pass as coverage
  of this one.

#### Scenario: A SECOND inherited name behaves correctly by the same mechanism

This is what distinguishes removing the channel from renaming the delimiter (issue AC7). Note
`constructor` and `toString` are inherited **writable data** properties rather than accessors, so
before the fix a `[[Set]]` of them *did* create an own property: they are therefore also the proof
that the fix did not regress the ordinary path.

- **GIVEN** a Cassandra-written table declaring quoted columns `"constructor"` and `"toString"`
- **WHEN** a row with both set is read through the Node binding
- **THEN** each is an own enumerable data property with the declared value, present in
  `Object.keys(row)`
- **AND** `row.constructor` is the column value, not `Object`
- **AND** the fix's source contains no comparison against any literal property name (asserted by
  review, and stated in the doc comment as the reason the mechanism was chosen).

#### Scenario: A column name a plain object cannot express is refused, not dropped

- **GIVEN** a column name the chosen mechanism cannot represent (under the `Property`/`CString`
  mechanism, a name containing an interior NUL byte)
- **WHEN** the row is converted
- **THEN** the conversion returns an **error** through the binding's one FFI error contract
- **AND** the column is never silently skipped — a dropped column with no diagnostic is the defect
  this change exists to remove, and a fix that reintroduces it in a corner is not a fix.

### Requirement: A JSON object cell inherits nothing

`json_to_napi` SHALL construct every JavaScript object it creates for a JSON object — at **every**
nesting depth — with a **null prototype**, so no key can reach an inherited accessor and
`obj[k] === undefined` means exactly "no such key". This matches the contract #3504 adopted for the
UDT field bag, which is the closest sibling surface: a mapping whose keys are data.

#### Scenario: A JSON object key named `__proto__` is a key

- **GIVEN** a `Value::Json` whose document is `{"__proto__": "v", "constructor": "c", "ok": 1}`
- **WHEN** it is converted to a JavaScript value
- **THEN** each key is an own enumerable data property with its declared value, and
  `Object.keys(obj)` contains all three
- **AND** `Object.getPrototypeOf(obj) === null` for EVERY JSON object, colliding or not — so the
  property is one of the CONSTRUCTION and not of the data
- **AND** a nested object at any depth has a null prototype too.

#### Scenario: A null-valued JSON `__proto__` key does not replace the prototype

- **GIVEN** a `Value::Json` whose document is `{"__proto__": null}`
- **WHEN** it is converted
- **THEN** `Object.hasOwn(obj, '__proto__')` is `true`, `obj['__proto__']` is `null`, and
  `Object.getPrototypeOf(obj) === null` — the same as for any other JSON object.

#### Scenario: The JSON half is reached from the PUBLIC surface, or its unreachability is measured

`Value::Json` is produced only by the schema-declared `json` custom type, which Cassandra has no
equivalent of, so the subject is a Cassandra-written `text` column read through a committed CQLite
schema declaring that column as `json`.

- **GIVEN** such a fixture and schema
- **WHEN** the column is selected through the Node binding
- **THEN** the scenarios above hold at the public surface (wiring evidence)
- **OR**, if that route provably does not reach `json_to_napi`, the change ships the
  null-prototype construction with Rust-level coverage **and** an explicit recorded measurement that
  the path has no public-surface reachability — never a CQLite-written round-trip presented as an
  oracle, which is invariant to a uniform error on both sides.

### Requirement: Column order and existing row semantics are unchanged

The change SHALL preserve every contract #1446 owns.

#### Scenario: `Object.keys(row)` still equals the SELECT column order

- **GIVEN** any result whose metadata covers every value
- **WHEN** rows are read through the Node binding
- **THEN** `Object.keys(row)` equals `result.columns.map(c => c.name)`, element for element and in
  order
- **AND** where extras exist, they follow in **name-sorted** order after the declared columns
- **AND** a metadata column with no matching value is still **skipped**, not null-filled — no
  phantom `col_0: null` appears beside the real cell.

#### Scenario: An ordinary row is observationally unchanged

- **GIVEN** a result from the existing corpus with no colliding column name
- **WHEN** read through the Node binding
- **THEN** `row.hasOwnProperty(name)`, `row.toString()`, `row instanceof Object`, `{...row}`,
  destructuring, `JSON.stringify(row)`, `Object.entries(row)` and `for…in` all behave exactly as
  before
- **AND** `udt_to_object`'s shape is unchanged: `{ typeName, keyspace, fields }` with a
  null-prototype `fields`.

### Requirement: The declared TypeScript surface states the contract

`bindings/node/lib/index.d.ts` SHALL document the row contract it now guarantees and SHALL type the
JSON-object cell, which the `Value` union does not currently include at all.

#### Scenario: `Row` and `Value` match the delivered behaviour

- **GIVEN** `bindings/node/lib/index.d.ts`
- **THEN** `Row`'s documentation states that every column is an own enumerable data property, that
  the prototype is `Object.prototype`, and that `Object.hasOwn(row, name)` (not `name in row`, and
  not truthiness) is the correct absence probe — because an inherited name still answers `in`
- **AND** `Value` includes the JSON-object shape, documented as a **null-prototype** object, so a
  consumer is told that `obj.hasOwnProperty` does not exist on it
- **AND** the existing `typescript-definitions.test.js` continues to pass.

### Requirement: The Python row path is asserted to have no analogous hole

The expectation is *no defect* — a Python `dict` has no inherited accessors and `__setitem__`
consults no prototype chain — and this change SHALL assert it rather than assume it. No Python
behaviour changes.

#### Scenario: A Python row with the colliding column names is complete

- **GIVEN** the same Cassandra-written row-collision fixture
- **WHEN** it is read through the Python binding
- **THEN** the row dict holds `__proto__`, `constructor` and `toString` as ordinary keys with their
  declared values
- **AND** the key set equals the selected column names exactly
- **AND** the null-valued case is a `None` value under the key, changing nothing about the dict.
