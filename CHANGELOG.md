# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **BREAKING (public API, source-level): `cqlite-core`'s `Value` enum gains a
  `Value::Empty(EmptyValueType)` variant (#3805)** — it carries the DECLARED CQL
  type of a zero-length multicell map cell-path key, renders as `""` at every
  egress surface and `EMPTY(<type>)` in `Display`, is refused fail-closed on
  write, and is a public-surface ADDITION, so any consumer with an exhaustive
  `match` over `Value` gains a required arm (it broke one in-fleet test,
  `cqlite-core/tests/issue_3722_udt_field_type_fidelity.rs`, until that arm was
  added).

- **BREAKING (CLI `--format json`, observable): a `decimal` and a `varint` render
  as UNQUOTED JSON NUMBERS, not as quoted strings (#3644).** Same egress and the
  same class as the #3629 entry below. `JsonCell::from_value`
  (`cqlite-cli/src/output/json_cell.rs`) now emits both types as raw JSON number
  fragments carrying `ValueFormatter`'s digits verbatim, at every position — a
  bare cell, and nested in a `list`/`set`/`tuple`, a map value or a UDT field.

  - **Before**: `{"amount":"123.45","big":"170141183460469231731687303715884105727"}`
  - **After**:  `{"amount":123.45,"big":170141183460469231731687303715884105727}`

  **Oracle**, read at the pinned tag — never CQLite's prior output:
  `cassandra-5.0.8`'s `DecimalType.toJSONString` (`DecimalType.java:314-317`) and
  `IntegerType.toJSONString` (`IntegerType.java:488-491`) both return
  `Objects.toString(getSerializer().deserialize(buffer), "\"\"")`, i.e. a bare
  `toString()`; `DecimalType` deliberately OVERRIDES the QUOTING form at
  `AbstractType.java:186-189`, so the absence of quotes is a decision Cassandra
  made explicitly. `tools/JsonTransformer.java:494` writes a cell value with
  `writeRawValue(cellType.toJSONString(...))`, so that text reaches the document
  unquoted.

  **What a consumer that called `as_str()` on these columns should do.** Read the
  cell as a NUMBER. A `decimal`/`varint` is arbitrary-precision (a Java
  `BigInteger` unscaled value — the committed
  `test_signed_coll.signed_special_collections` fixture carries 33 significant
  digits), so a parser that maps every JSON number onto a `double` will LOSE
  digits it previously received intact inside the string. Use a parser that keeps
  the LITERAL:

  - **Python**: `json.loads(text, parse_float=decimal.Decimal)`. `parse_float`
    receives the number's original lexeme, so no rounding has happened yet. A
    `varint` needs nothing — it carries no `.`/`e`, so it goes through
    `parse_int`, and a Python `int` is already arbitrary-precision.
  - **JavaScript**: a bigint/decimal-aware parser such as `lossless-json` (every
    number is kept as its original text) or `json-bigint`. **A plain
    `JSON.parse` reviver does NOT work**: the reviver's `value` argument is an
    already-parsed, already-rounded `Number`, so the digits are gone before it is
    called and cannot be recovered from it. (The ES2025 source-text-access
    reviver — the third `context.source` argument — *does* expose the original
    lexeme, but it is not available in every engine — measured here, Node.js 20
    has no `context.source` (it landed in V8 with Node.js 21) — so feature-detect
    it and fall back to a dedicated parser.)
  - **Rust**: `serde_json`'s `arbitrary_precision` feature (a `Number` then holds
    the literal digits) or `serde_json::value::RawValue` (the raw fragment,
    unparsed). Note `arbitrary_precision` is additive across the whole build, so
    it changes `Number` for every `serde_json` user in your dependency graph;
    `RawValue` is the local, non-invasive option.

  The digits themselves are unchanged — only the quotes are gone.

  **Also changed, in `cqlite-core`'s text formatter and therefore in ALL text
  egress (`json`, `csv`, `table`): a `decimal` whose magnitude is ZERO at a
  NEGATIVE scale now renders `0e1` (scale `-1`) instead of `00`.**
  `ValueFormatter::format_decimal` (`cqlite-core/src/util/value_fmt.rs`) spelled
  that value `"0"` followed by one zero per unit of negative scale, which is not
  valid JSON — a leading zero followed by a digit — so the JSON egress had to
  quote it. It now takes the SAME bounded exponent form `<digits>e<-scale>` that
  the issue #1754 over-bound branch already emitted, which is a valid JSON number
  and preserves the scale (`BigDecimal(0, -1)` is not `BigDecimal(0, 0)`, so
  collapsing to `0` would discard information). A NON-zero magnitude at a negative
  scale was already valid JSON and is unchanged (`5` at scale `-1` → `50`).
  **This is a JSON-VALIDITY fix, NOT spelling parity**: `0e1` is still not Java's
  `BigDecimal.toString()` spelling `0E+1`, and the wider `format_decimal` vs
  `BigDecimal.toString()` divergence class (a non-zero magnitude at a negative
  scale → Java `1.23E+3` vs CQLite `1230`; an adjusted exponent below −6 → Java
  `1E-10` vs CQLite `0.0000000001`) is untouched and tracked separately — no
  committed fixture covers it, and it would move `csv`/`table` output for many
  more values. Pinned by
  `cqlite-core/tests/issue_3644_decimal_zero_negative_scale.rs` and
  `cqlite-cli/tests/issue_3644_json_decimal_unquoted.rs`
  (`a_zero_magnitude_at_a_negative_scale_is_an_unquoted_exponent_form`).

  **Known residual**, recorded rather than omitted: the quoted-string fallback
  remains, and after the fix above the only rendering known to take it is the
  `<corrupt-decimal:…>` marker an over-bound magnitude produces (issue #1754).
  That is not a JSON number, so the egress emits it as a string rather than an
  unparseable document; it deliberately does not invent a spelling.

  Not affected by the QUOTING change (their text was never quoted; the
  zero-at-negative-scale spelling above does reach them): `table` and `csv`
  output, and
  `cqlite-core`'s `impl ToJson for Value`, which is a different renderer and still
  emits `{"scale": …, "unscaled": <base64>}` — see
  `docs/development/QUERY_RESULT_CONTRACT.md`.

- **BREAKING (CLI `--format json` and `cqlite-core`'s `ToJson`, observable): a UDT
  renders as its DECLARED FIELDS AND NOTHING ELSE — the injected `_type` key is
  gone (#3629).** This is the CLI-side half of #3504's class, on a third surface and
  in **two independent code copies**: `cqlite-cli/src/output/json.rs`'s
  `value_to_json` and `cqlite-core/src/query/result.rs`'s `impl ToJson for Value`
  each inserted `"_type"` into the SAME JSON object that then received the UDT's own
  declared fields. A UDT that DECLARES a field named `_type` (legal CQL via a quoted
  identifier) therefore silently **overwrote** the injected type name, and a NULL
  such field rendered it as `null` — so the type name was unrecoverable and the
  collision was invisible. Both copies now call one shared rule,
  `cqlite_core::util::udt_json::udt_to_json_object`, generic over the field-VALUE
  renderer because the two writers differ deliberately in 11 other arms.

  - **Before**: `{"_type":"plain","label":"no-colliding-field","real_field":7}`
  - **After**:  `{"label":"no-colliding-field","real_field":7}`

  **What a consumer that parsed `_type` should do.** `--format json` carries **no
  type channel by design**: the document is a bare array of row objects (an empty
  result is exactly `[]`) and column metadata supplies only key NAMES, so there is
  nowhere in that shape for type identity to live. Read the type name from the
  **schema** (`DESCRIBE TYPE` / the `CREATE TYPE`), or use a **binding**, where
  #3504 put identity on the object OUTSIDE the field mapping (`type_name` /
  `keyspace`). The new shape is what the reference tool already emitted:
  `cassandra-5.0.8`'s `UserType.toJSONString` iterates `stringFieldNames` alone and
  writes no type key and no keyspace key, appending literal `null` for an absent
  field — which is also why a UDT whose own `_type` field IS null still correctly
  renders `"_type": null`.

  **Known coverage reduction**, recorded rather than omitted: the JSON/CSV
  golden-parity lane's UDT comparator lost the check that compared the emitted type
  name against the committed `CREATE TYPE`, so it can no longer detect a UDT
  resolved against the WRONG type when two types declare the same field names,
  order and types. That is unavoidable — the egress no longer carries type identity
  for a comparator to check — and the old code was already blind on that fixture,
  refusing outright to compare any UDT declaring a `_type` field.

### Fixed

- **All surfaces (observable): a structured value inside a frozen UDT decodes from
  its DECLARED type instead of degrading to a blob (#3631).** A UDT field whose
  type is a collection, tuple or nested UDT — `frozen<map<text,int>>`,
  `frozen<list<…>>`, `frozen<set<…>>`, `frozen<tuple<…>>` — surfaced to every
  caller as raw bytes. The decode path matched a CLOSED SET of primitive types and
  fell back to `Value::Blob` for the rest, while the declared `CqlType` naming the
  real type was the `match` scrutinee itself: the silent degradation #28
  (no-heuristics) forbids.

  - **Before** (`udt_hashable_shapes` row 3's `stn`, CLI `--format json`):
    `{"label":"unhashable","m":"0x0000000100000001610000000400000001"}`
  - **After**: `{"label":"unhashable","m":[{"key":"a","value":1}]}` — which is what
    Cassandra's own `sstabledump` already emitted for those bytes (`"m":{"a":1}`).

  Python gets `{'a': 1}` instead of a 17-byte `bytes`; `cqlite-core`'s `ToJson`
  gets `{"'a'": 1}` (its `Map` arm `Display`-stringifies keys, unchanged here).

  **Two consequences a caller may notice.** (1) A declared type CQLite has no
  decoding rule for is now an explicit `Error::unsupported_format` NAMING the type,
  where it used to be an opaque blob and a `tracing::debug!` no caller could see —
  including a nested UDT whose field list is in neither the `UdtRegistry` nor the
  inline type. (2) In the Python binding, `Udt.__hash__`'s `TypeError` is now
  reachable from decoded data: a UDT with a decoded `dict` field is genuinely
  unhashable, where the same UDT with a `bytes` field was not. No column in the
  committed corpus reaches a hashing position with such a UDT inside it, so nothing
  observable changed there — the boundary is recorded in
  `test-data/fixtures/issue_3504/README.md`, Table 2.

  Also fixed in the same class: a ZERO-LENGTH field followed Cassandra's per-type
  empty-value rule rather than becoming an empty blob (`Value::Null` for every
  scalar whose serializer guards `accessor.isEmpty(value) ? null : …`; only
  text/ascii/varchar/blob keep a meaningful empty value); a `date`-typed UDT field
  gained the `SimpleDateType` epoch offset every other decode site already applied;
  and the type-nesting limit no longer resets at each frozen-UDT hop, so a cyclic
  `UdtRegistry` is refused rather than recursing until the stack is exhausted.
  Instance A of the same issue — a non-frozen `map<frozen<udt>,int>` cell-path key —
  was fixed separately by #3612.

  **Marshal-form UDT fields that never decoded at all.** A UDT read from its on-disk
  `UserType(...)` marshal string resolved each field type through a table that
  covered 16 marshal suffixes, so a field Cassandra CAN write — `duration`
  (`DurationType`), `smallint` (`ShortType`), `tinyint` (`ByteType`), `counter`
  (`CounterColumnType`) or a `tuple<…>` (`TupleType(...)`, which is STRUCTURAL and
  needs parsing, not a name match) — surfaced as a blob. All of them now decode; the
  table is one arm per `CQL3Type.Native` constant at the pinned `cassandra-5.0.8`
  tag, and there is now ONE such table rather than two that disagreed. Two mappings
  were also WRONG: legacy `DateType` (an 8-byte millis value, `asCQL3Type() ->
  TIMESTAMP`) was read as CQL `date` because a `ends_with("DateType")` arm also
  matches `SimpleDateType`, and `TimeUUIDType` was collapsed onto `uuid`. Matching is
  now EXACT on the marshal class's simple name, so a third-party `AbstractType` is no
  longer suffix-matched into `blob`. A marshal type CQLite genuinely cannot express
  (`EmptyType`, `VectorType(...)`, a foreign class) is refused BY NAME instead of
  being reported as a user-defined type with a missing field list.

  **And the marshal class's PACKAGE is part of its identity.** Matching the simple
  name exactly still ignored the package, so a third-party `com.acme.Int32Type` was
  decoded as CQL `int` — an unknown class's bytes read as if the class were known,
  the same no-heuristics defect one level up. `TypeParser.getAbstractType` at the
  pinned tag resolves a name as `compareWith.contains(".") ? compareWith :
  "org.apache.cassandra.db.marshal." + compareWith`, so a marshal name has exactly
  two legal spellings — bare, or fully qualified under
  `org.apache.cassandra.db.marshal` — and both continue to decode identically.
  Anything else, INCLUDING a structural form (`com.acme.TupleType(...)`) and a
  package that merely resembles the marshal one (`…db.marshalX.`,
  `notorg.apache.cassandra.db.marshal.`, `my.org.apache.cassandra.db.marshal.`), is
  now an `Error::unsupported_format` naming the package it was rejected on. The same
  rule closed a package-SUFFIX hole in the `UserType(` marker locator, where a
  substring search accepted `my.org.apache.cassandra.db.marshal.UserType(...)` as
  Cassandra's.

  **A malformed `inet` is refused.** An `inet` value is 4 bytes (IPv4) or 16 (IPv6),
  or empty meaning null (`InetAddressSerializer`); any other width was accepted and
  handed back as a `Value::Inet` no address can be built from. It is now
  `Error::corruption`, the same class and wording every other bad-width scalar gets.

- **Both bindings (observable): a JSON number above `i64::MAX` no longer loses
  precision, and neither binding fabricates a substitute value (#3505).** A
  `Value::Json` cell's number was classified inline in each binding as
  `as_i64()` → `as_f64()` → *fallback*. For a legal JSON integer above
  `i64::MAX`, `as_i64()` returns `None` and **`as_f64()` succeeds lossily**
  (`u64 → f64` has 53 mantissa bits), so `18446744073709551615` was delivered as
  `1.8446744073709552e19`. The `fallback` arm was unreachable in both bindings.
  - The classification now lives ONCE, in `cqlite-ffi-common::json_number`, as
    `i64` → `u64` → `f64` → refusal. Adding the `u64` arm is the whole fix: it
    makes the `f64` arm reachable only for the `Float` variant, where the
    conversion is exact.
  - **Python**: a `u64`-range integer is an exact `int` (Python integers are
    arbitrary precision). The old `n.to_string()` fallback — which shifted the
    host type from a number to a `str` — is gone.
  - **Node**: a `u64`-range integer is a `BigInt`, the type this binding already
    used for an `i64` outside `i32` range. The old fallback returned
    `env.get_null()`, delivering a **fabricated `null`** for an unrepresentable
    number, indistinguishable from a genuine JSON `null`; it is now a typed
    `PARSE`/`Data` error.
  - `serde_json`'s `arbitrary_precision` is deliberately **not** enabled; the
    decision and its three reasons are recorded in
    `cqlite-ffi-common/src/json_number.rs`.
  - Residual, out of reach at this layer (**#3636**): an integer literal outside
    `[i64::MIN, u64::MAX]` is collapsed to `f64` by `serde_json`'s **parser**
    before any CQLite code runs, so it still arrives rounded. Enabling
    `arbitrary_precision` alone does NOT fix it — under that feature `as_f64`
    parses the stored string, so such a literal still classifies `F64` lossily.
    A real fix additionally needs an exact-integer parse of `Number::as_str()`
    placed BEFORE the `as_f64()` arm.
  - Reachability, stated honestly: `Value::Json` requires a `"json"` comparator
    and no fixture in `test-data/` has one, so this path is unreachable from
    today's corpus.
  - **Wiring evidence** (`JSON_NUMBER_VECTORS`, the #1452 mechanism): unit tests
    on the shared classifier do NOT prove either binding CALLS it — the mutation
    "make the `U64` arm `u as f64`" originally reddened nothing in the
    repository. A committed cross-binding table of JSON number literals is now
    driven through each binding's PRODUCTION dispatch
    (`value_to_py`/`value_to_napi` → `json_to_*` → `json_number_to_*`) by
    `cqlite._json_number_from_text` / `_jsonNumberFromText`, and both suites
    assert the rendered text AND the host type. In JS the type half is the
    load-bearing one: `String(9223372036854775808)` is identical for a lossy
    double and an exact `BigInt`.

- **Python parity harnesses: `values_equal` no longer masks int/float precision
  loss (#3505).** Both copies coerced a mixed `int`/`float` pair through
  `float()`, which rounded the EXACT side down to the LOSSY side — so the bug
  above was invisible to the harness that should have caught it. The coercion is
  now bounded at `2**53`: below it every integer is exactly representable in an
  IEEE-754 double so the tolerant compare genuine `FLOAT`/`DOUBLE` columns need
  is provably lossless and is retained; strictly above it (`2**53` itself is
  exactly representable, so it stays on the tolerant side) the comparison is
  exact.
  `bool` is excluded in both directions (`isinstance(True, int)` is `True`, so
  `True` and `1.0` compared equal). The rule was duplicated in
  `test_cli_parity.py` and `test_parity.py` and now lives once in
  `bindings/python/tests/numeric_compare.py`.
  `bool` vs `Decimal` is rejected too, not only `bool` vs `int`/`float`:
  `Decimal(1) == True` is `True` in Python, so the first pass left the `Decimal`
  dispatch open.
  A second degeneracy in the same formula is closed in both languages: with an
  infinite operand `abs(a-b) <= max(rel_tol*max(|a|,|b|), abs_tol)` reduces to
  `inf <= inf`, so EVERY finite value compared equal to `Infinity` and `+inf`
  compared equal to `-inf` — real values for a CQL `float`/`double` column. Two
  genuine equal infinities still match.
  Node's `parity-utils.js` did NOT have the int/float mask — its
  `bigint`↔`number` arms were already exact — but `BigInt(x)` threw `RangeError`
  on a non-integer `number`, crashing the harness instead of reporting a
  mismatch; hardened.
  `test_parity`'s tolerant branch keeps its pre-#3505 ASYMMETRY (entered only
  when the binding side is a float), so an `int` binding value against a `float`
  golden stays an exact comparison — a change that removes a mask must not widen
  a golden-file oracle as a side effect.

### Changed

- **BREAKING (both bindings): a UDT's type identity is carried OUT OF BAND, so a
  UDT field named `_type`/`_keyspace` displaces nothing (#3504).** Both bindings
  rendered a UDT as ONE flat namespace holding the injected type identity and the
  UDT's own declared field names, markers written first — `udt_to_py` did
  `set_item("_type")`, `set_item("_keyspace")`, then `set_item(field.name)`, and
  `udt_to_object` did the identical thing. A UDT that DECLARES a field named
  `_type` or `_keyspace` (legal CQL via a quoted identifier) therefore silently
  **overwrote** the marker, and the type name became unrecoverable from the
  result; a NULL such field nulled it outright. That is a control marker placed in
  a namespace the data controls, so the fix removes the channel rather than
  picking a rarer marker.

  - **Python**: a UDT is now a `cqlite.Udt` (frozen `#[pyclass]`, exported from
    the module and declared in `__init__.pyi`) with `type_name` / `keyspace` /
    `fields`. The mapping protocol is retained and delegates to `fields`, so
    `udt["street"]`, `"city" in udt`, `len(udt)`, `iter(udt)` and
    `keys`/`values`/`items` keep working. `__eq__`/`__hash__` are over
    `(keyspace, type_name, fields)`.
  - **Node**: a UDT is now `{ typeName, keyspace, fields }`, with the declared
    fields in the nested `fields` object. `interface UdtValue` loses its
    `[field: string]: Value` index signature — that signature is what permitted
    the collision. **`fields` has a NULL PROTOTYPE** (`Object.create(null)`):
    a plain object's property assignment consults the prototype chain, so a UDT
    field named `__proto__` — legal CQL via a quoted identifier, exactly like
    `_type` — reached `Object.prototype`'s inherited accessor instead of becoming
    a field (measured on the fixture: a string value VANISHED, a null value
    REPLACED the field bag's prototype). Inheriting nothing removes that channel
    for every name rather than special-casing one. Every read shape is unchanged
    (indexing, `in`, `Object.keys`/`entries`, spread, `JSON.stringify`); the one
    difference is that `fields.hasOwnProperty(...)` no longer exists — use
    `Object.hasOwn(fields, name)`, which is the correct form for a name-keyed bag
    regardless.
  - **`value_to_hashable_key`'s `Udt` arm (Python)** projected a UDT to a
    `frozenset` holding a pair for `_type`, one for `_keyspace`, then one per
    field, so a field named `_type` produced a **duplicate** `_type` pair that
    nothing deduped (measured on the new fixture: pair names
    `['_keyspace', '_keyspace', '_type', '_type', 'real_field']`). It now emits a
    `Udt` — exactly one entry per declared field, none for the metadata — and
    identity participates in equality/hash, so two UDTs of different declared
    types with identical fields remain distinct `dict` keys. `Tuple`/`Set` arms
    are deliberately still absent (#3500).
  - **Projection totality WIDENED as a side effect, measured rather than
    assumed.** Because a `cqlite.Udt` is HASHABLE where the old `dict` was not, a
    UDT reached through the arm-less `Tuple` fall-through in a hashed position now
    reads successfully: `set<frozen<tuple<frozen<udt>, int>>>` and
    `map<frozen<tuple<frozen<udt>, int>>, int>` both raised
    `TypeError: unhashable type: 'dict'` before and now yield a `frozenset` /
    `dict` keyed by `(Udt, …)`. `set<frozen<set<frozen<udt>>>>` still raises
    `TypeError: unhashable type: 'list'`, unchanged, because a UDT-bearing set
    renders as a Python `list` for CLI parity (#804) — a different cause. This is
    NOT "#3500 is fixed": no arm was added. Boundary pinned by
    `test_udt_collision.udt_hashable_shapes` in the fixture.

  **Migration.** Python: `udt["_type"]` → `udt.type_name`, `udt["_keyspace"]` →
  `udt.keyspace`, `isinstance(v, dict)` → `isinstance(v, cqlite.Udt)`; field
  access is unchanged. Node: `result._type` → `result.typeName`,
  `result._keyspace` → `result.keyspace`, `result.street` →
  `result.fields.street`. Reading a marker out of the field namespace is the ONLY
  thing that stops working, and that is the deliverable: `udt["_type"]` now
  reaches a FIELD of that name (`KeyError` when none is declared) and
  `result._type` is `undefined`.

  Node's shape is a plain object and Python's a dedicated class because each
  binding already had an established idiom for a value type (Python's
  `cqlite.Duration` is a `#[pyclass]`; Node's `Duration` is a plain object, with
  napi classes reserved for handles). The spelling differs by language convention
  — PyO3 exposes snake_case, napi-rs camelCases — and the semantics are identical.

  Subject: `test-data/fixtures/issue_3504/`, a **Cassandra 5.0.2-written** SSTable
  declaring `CREATE TYPE collide ("_type" text, "_keyspace" text, "__proto__"
  text, real_field int)` (Cassandra accepts all three as quoted identifiers). No pre-existing corpus fixture declared such a field, so the defect had
  no test subject. The CLI is deliberately NOT changed here: its JSON writer still
  injects `_type`, and it is the binding parity suites' comparison ORACLE — moving
  an oracle in the same change as its subject is how a guard goes blind. Tracked
  as a follow-up on #3504 and recorded in `docs/development/M4_spec.md` §5.3.

- **Node binding (observable): a malformed `inet` cell is a typed `PARSE` error
  on BOTH read paths, and `execute()` no longer returns `null` for one
  (#1452).** Two defects found reviewing the shared-crate extraction:
  - `executeNative()` mapped the shared crate's malformed-length error with a
    bare napi error carrying no contract metadata, so `lib/error-wrapper.js` fell
    back to its defaults and a corrupt SSTable inet cell surfaced as
    `code: 'INTERNAL'`, `category: 'Internal'` — an internal-bug identity for a
    data fault. It is now `code: 'PARSE'`, `category: 'Data'`,
    `isRecoverable: false`, matching the sibling DECIMAL refusal and the one
    #1451 error contract.
  - The DEPRECATED `execute()` path kept a second, private inet 4/16 length
    dispatch whose malformed-length branch produced a JSON `null` —
    indistinguishable from a genuine NULL, i.e. silent data loss — while
    `executeNative()` raised on the same cell. It now uses the shared dispatch
    and **throws** that same typed error; a malformed cell nested in a
    `list`/`set`/`map`/`tuple`/`frozen`/UDT propagates rather than being
    flattened to `null`.

  The Python binding is unaffected: its inet arm already raised, and its
  exception class stays `ParseError` (routing it through the contract table would
  change the class, deferred deliberately).

- **BREAKING (Python binding, observable): CQL `decimal` now has ONE rendering
  policy across both language bindings (#1452, epic #1434).** The byte-math both
  bindings share was extracted into a new internal crate `cqlite-ffi-common`,
  which holds the single implementation of `decimal`, `varint` and `inet`
  rendering. Because the two DECIMAL implementations had been hardened
  independently (#1741 Python, #1754 Node) they had come to **disagree on
  observable output for inputs both accepted**; converging them required picking
  one, and the #1754 policy was chosen for both:
  - **Refusal ceiling: 32 KiB of unscaled magnitude.** Above it, a typed
    corruption error (`CqliteError` in Python, `code: "PARSE"` in Node) naming the
    scale, the unscaled length and the ceiling. Below it the render is
    **infallible**.
  - **Exponent form** (`<digits>e<-scale>`, every digit preserved) instead of a
    positional expansion when the magnitude exceeds 1024 bytes or `|scale|`
    exceeds 1,000,000.
  - **What changes for Python callers:** a large-but-well-formed value no longer
    raises. The previous guard refused any magnitude whose digit count exceeded
    `sys.get_int_max_str_digits()` (default **4300**), because the old code called
    Python `str()` on the unscaled *Python int* — an uncatchable `ValueError`. Rust
    renders the digits now, so that failure mode is structurally gone. Concretely:
    a 2000-byte unscaled magnitude with `scale = 3` (4817 digits) and
    `scale = i32::MIN` **used to raise `CqliteError` in Python while the Node
    binding rendered them**; both now render, in both bindings. The guarantee the
    guard existed for (#1437/#1440 — a corrupt SSTable raises a typed, catchable
    error and never aborts the interpreter) is **preserved**.
  - **What changes for Node callers:** nothing, except that a *well-formed* value
    with `65535 < |scale| <= 1_000_000` no longer **panics**. The #1754 positional
    branch fed `scale` to a `{:0>width$}` format spec, and `core::fmt` packs the
    width into a `u16`, so such a scale raised "Formatting argument out of range"
    on the render path. The padding is now built explicitly.

- **`cqlite_core::observability::error_schema::ErrorCategory` is renamed
  `ObsErrorCategory` (#1452, for #1705).** `cqlite-core` had two
  error-handling-relevant enums both named `ErrorCategory`, distinguished only by
  import line, both stored in a field called `category`, and genuinely
  disagreeing (`QueryTimeout` is `Query` in one and `Timeout` in the other). The
  **telemetry** one is renamed; `cqlite_core::error::ErrorCategory` — what
  `Error::category()` returns and what the FFI error contract mirrors — is
  **unchanged**, as is `cql::error`'s module-local enum. One of the two being
  uniquely named is what makes a wrong import fail to compile instead of
  mis-classifying. Re-exported as `cqlite_core::observability::ObsErrorCategory`.

### Removed

- **BREAKING (public API): `cqlite_core::ffi_error_contract` is gone; the table
  moved to `cqlite_ffi_common::error_contract` (#1452).** The #1451 FFI error
  contract (`PyExceptionClass`, `FfiErrorRow`, `FfiErrorVariant`, `variant_of`,
  `contract_for`) is pure binding-facing data and now lives in the shared FFI
  crate, which its own module doc had pre-authorised. The move is
  behaviour-preserving: the rows, the exhaustive `variant_of` match over
  `Error`, the fail-closed `from_name` and the compile-time obligation that a new
  core `Error` variant cannot ship without a row all carry over unchanged, and the
  table's test suite moved with it and passes unmodified. The `category` column
  still mirrors `Error::category()`.
  - **No deprecated re-export is left in `cqlite-core`, deliberately.** Two paths
    to one item is the failure shape this change removes. **Migration:** replace
    `cqlite_core::ffi_error_contract::X` with
    `cqlite_ffi_common::error_contract::X`; no type or function names changed.
  - Note for reviewers: the gate's `pub-surface` component checks
    declaration/inner-`cfg` consistency and is **not** an API-drift detector
    (#1712/#3366), so nothing flags this mechanically — hence this entry.
- **BREAKING (public API): decorative configuration knobs are deleted (#1696,
  epic #1685 "config honesty" / audit finding AH3).** Every knob below had ZERO
  production readers: setting it changed nothing, silently. A deleted field on
  `cqlite_core::Config` is now a COMPILE error for an embedder, which is the
  loudest signal available and the intended posture.
  - `cqlite_core::config::StorageConfig`: `max_sstable_size`, `block_size`,
    `enable_bloom_filters`, `bloom_filter_fp_rate`, `io_threads`, `sync_mode`
    (and with it the now-unreferenced `SyncMode` enum).
  - **On the two bloom knobs specifically, stated precisely because both loose
    versions are wrong:** the bloom-filter path EXISTS, is tested AND is WIRED —
    `cqlite-core/src/storage/sstable/bloom.rs` is a real Cassandra-parity
    `BloomFilter` whose double-hashing operand order and `Filter.db` binary
    layout are both verified against `BloomFilterSerializer.java` /
    `OffHeapBitSet.java`, and the production point-read paths DO consult a loaded
    `Filter.db` (`reader/component_loading.rs` loads it;
    `reader/partition_lookup.rs` / `reader/partition_successor.rs` prune an
    SSTable on `might_contain == false`). So this removal does NOT say the bloom
    path is unwired. What had zero production readers is the two CONFIG KNOBS:
    bloom behaviour follows from the SSTable's own `Filter.db`/schema metadata,
    never from a knob, so `enable_bloom_filters` could not switch anything on or
    off and `bloom_filter_fp_rate` sized no filter. They are deleted rather than
    given a consumer speculatively, because a knob should arrive WITH its
    consumer. **#2632** ("wire folded murmur3 h2 into bloom-filter (`Filter.db`)
    plumbing") is the open issue that would introduce a bloom knob WITH one.
  - `cqlite_core::config::QueryConfig`: `plan_cache_size`, `enable_optimization`,
    `parallel` (and with it the `ParallelQueryConfig` struct).
  - `cqlite_core::config::Config::performance` entirely, with the
    `PerformanceConfig` and `BackgroundTaskConfig` structs.
  - The `Config::validate` arms that judged deleted fields (`block_size`,
    `bloom_filter_fp_rate`) went with them: validating a field nothing reads is
    theatre.
  - **CLI config-file keys**: the whole `[connection]` section
    (`timeout_ms`/`retry_attempts`/`pool_size` — CQLite reads local files and
    never opens a network connection), `output.pager` (nothing ever spawned a
    pager) and `output.timestamp_format` (no formatter ever read it).
  - **Migration (CLI files): a config that still names a removed key STILL
    LOADS.** The CLI surface is a file, not a Rust type, so the posture is
    *parse-and-ignore PLUS a named deprecation warning* rather than
    `deny_unknown_fields` — hard-failing would break every user who copied our
    own shipped `examples/example-config.toml`, which named all three. On load,
    each still-present removed key is reported by name on stderr, so a dead
    setting can no longer look like a live one. Delete the keys to silence it.
  - **Migration (embedders writing Rust):** drop the field assignment. None of
    these knobs had an effect, so there is no behavior to preserve and no
    replacement to adopt.
  - **Migration (Python / any JSON or dict config surface): the old shape still
    DESERIALIZES, and now WARNS.** A Rust embedder gets a compile error, but the Python
    bindings' dict/JSON bridge is a `serde_json::from_str`, and serde DISCARDS
    unknown fields — so a saved pre-change config naming `performance`,
    `storage.block_size`, `query.parallel` and the rest deserialized
    successfully and was silently ignored. `cqlite_core::Config::from_json_str`
    (and the bindings on top of it) now report every removed key the document
    still sets: a Python `UserWarning` naming each dead path, raised only once
    the operation has SUCCEEDED — validation included, so a document that names a
    removed key AND carries an invalid surviving value gets the rejection alone
    and no deprecation warning about a config that never took effect.
    **The warning text itself makes no claim about whether the load succeeds**,
    and that is deliberate: it names the dead keys and says they have NO EFFECT,
    nothing more. It had said "they are IGNORED — the configuration still loads",
    which is a promise about a LATER stage, and review found it false three times
    running — each fix moved the emission one stage later and the next stage
    falsified it again (the CLI's `to_core_config` rejects
    `memory_limit_mb = 1` beside `cache_size_mb = 64` after the assurance has
    already printed). There is always a later stage, so no placement can make
    such a promise safe; a warning that reports only what it knows cannot be
    wrong about anything else. `UserWarning` and not
    `DeprecationWarning` because Python HIDES the latter under its default
    filters (shown only from `__main__` or under `-W`), which would have left the
    signal silent for an ordinary caller. Same posture as the CLI file surface,
    one posture crate-wide: parse-and-ignore PLUS a named warning, never
    `deny_unknown_fields`, which would hard-fail an existing caller with no
    migration path over keys that never did anything.
  - **Known residual, stated because the rule above is NOT universal (#3520):**
    the removed-key report is enforced on the CLI config-file loader, the Python
    bindings entry points, and Rust field access (a compile error, Rust callers
    only). It is NOT enforced on a DIRECT serde deserialization of
    `cqlite_core::Config` — `serde_json::from_str::<Config>` /
    `from_value::<Config>` bypass the reporting constructors and still discard
    removed keys silently. Enforcing it at the serde boundary needs a custom
    `Deserialize` capturing unknown keys across every nested config struct, which
    is tracked as #3520 rather than absorbed here.

- **BREAKING (public API): the schema JSON exporter and the never-compiled CQL
  generator are deleted (#1715, epic #1688 / audit finding AK4; ~2.0k LOC).**
  Owner-DECIDED delete (capstone ledger #9, 2026-07-01) — the surface had zero
  callers anywhere in the workspace and no Cassandra-side parity coverage.
  - Removed `cqlite_core::schema::json_exporter` and the 14 types it re-exported
    through `cqlite_core::schema` under the `experimental` feature
    (`JsonExporter`, `JsonExportConfig`, `JsonFormat`, `JsonSchema`, `JsonTable`,
    `JsonColumn`, `JsonPrimaryKey`, `JsonClusteringKey`, `JsonIndex`, `JsonUDT`,
    `JsonMetadata`, `JsonTableOptions`, `JsonPerformanceMetrics`,
    `JsonValidationResults`).
  - Removed seven `SchemaRegistry` methods —
    `export_schema_json`, `export_schema_json_with_config`,
    `export_schema_json_compact`, `export_schema_json_openapi`,
    `export_schema_json_pipeline`, `export_multiple_schemas_json` and
    `export_keyspace_schemas_json` — and two `SchemaDiscoveryEngine` methods,
    `export_json` and `export_json_with_config`.
  - **All nine existed in DEFAULT builds too**, as
    `#[cfg(not(feature = "experimental"))]` stubs that unconditionally returned
    `Error::UnsupportedFormat`. So this removal narrows the DEFAULT public API,
    not only the `experimental`-gated one — that is the breaking part, and it is
    called out here rather than left to be discovered at upgrade time. No caller
    could ever have depended on a successful result from any of them, since
    without `experimental` they could only ever return an error.
  - Removed `cqlite-core/src/schema/cql_generator.rs`, which had **no `mod`
    declaration anywhere in the repository** and was therefore never in any build
    graph; the file's only repo-wide mention was a doc comment. Any past change to
    it was a no-op.
  - **Migration:** there is no replacement, by design. A `schema export --json`
    surface is a future product decision that would restore this code from version
    control alongside a real design; it is deliberately not preserved as deprecated
    shims. Callers needing schema JSON today should serialize the public
    `TableSchema` / `SchemaInfo` types themselves.
  - The `experimental` feature flag itself is unchanged — it still gates
    `Database::flush()`/`compact()`, the INSERT executor path, bloom-filter tests
    and the `Storage::put`/`delete` stubs.

### Added

- **`storage.direct_io_memory_fraction` is now VALIDATED (#1696).** It was live
  but unvalidated: the reader silently CLAMPED nonsense (`<= 0.0`, NaN and the
  infinities fell back to the `0.5` default; anything `> 1.0` was pinned to
  `1.0`), so an operator who wrote `2.0` got the default and no word about it.
  `Config::validate` now rejects anything outside the documented `(0.0, 1.0]`,
  naming the knob and the offending value — and the rejection is REACHABLE from
  the public surfaces that were doing the clamping: `Database::open` validates
  the config it is handed (a failure mode that method already documented but
  never checked), and `SSTableReader::open` — reachable without a `Database` —
  enforces the range itself as the FIRST thing it does, before any `tokio::fs`
  call — so a missing or unreadable file cannot mask an invalid config behind an
  I/O error, and the caller is told about the problem it actually has. `1.0` is
  legal; `0.0` is
  REJECTED rather than read as "never use direct I/O", because a zero threshold
  makes every nonempty file exceed it, i.e. it reads as "never" and behaves as
  "always" (say `disk_access_mode = Direct` for always, `Mmap`/`Buffered` for
  never). A tiny/subnormal positive fraction is legal and honoured literally.
- **A standing knob-behavior guard (#1696):**
  `cqlite-core/tests/config_knob_behavior_guard.rs`. Every leaf field of the
  public config structs must be registered with either a set-knob →
  assert-observable-difference test, or an explicit reason why no observable
  difference is expressible. The registry is checked against `src/config.rs` in
  BOTH directions, so a NEWLY ADDED public knob with no evidence entry fails the
  build rather than joining the backlog that created epic #1685.

### Fixed

- **A dead producer no longer completes a query SUCCESSFULLY with a silently
  truncated result set (#3106).** Both channel boundaries behind the
  single-generation read path (used by the Flight `do_get` row route) treated
  "sender dropped" as "the scan finished", so a producer thread/task that UNWOUND —
  a panic anywhere in the walk or decode, rather than an `Err` return — ended the
  request with **fewer rows than the table holds, no error and no log**.
  - The query row stream's producer→consumer protocol now has an EXPLICIT terminator
    (`Done`) and a distinct `Failed` message; a disconnect WITHOUT a terminator is an
    `Error::Internal` naming the truncation, never a clean end of stream. The
    producer thread additionally runs under `catch_unwind`, so a panic reaches the
    client as an informative error carrying the panic message.
  - The batched streaming scan's driver task is now JOINED when its channel closes
    (`BatchedScanStream`), so a task that panicked/was cancelled surfaces as an
    `Error::Internal` instead of an empty-but-successful end of stream. This closes
    the same hole for the streaming `SELECT` and aggregate-fold paths, which
    consume the same surface.
  - Both errors are `is_recoverable() == false`: the failure is deterministic, so a
    retry would reproduce it.

- **The same hole on the multi-generation (query-engine full scan) read path (#3124).**
  Five more producer boundaries on the ≠1-generation path discarded their task's
  `JoinHandle` and read a closed channel as "the scan finished": the fan-out k-way
  merge task, each per-generation per-row sub-scan, the per-row → batch re-chunker's
  source, the windowed forwarder, and the cross-generation reconciling merge. A panic
  in any of them returned a silently SHORT result set with no error. All five now pair
  the channel with the producer task (the #3106 mechanism, generalised over the item
  type as `JoinedStream<T>`), so a dead producer is an `Error`, never end-of-stream.
  - The cross-generation reconciling merge was worse than truncation: its setup
    reported a dead task and a genuine `KWayMerger` CONSTRUCTION failure as the SAME
    error, and `SSTableManager::scan_stream` answers a construction failure by falling
    back to the non-reconciling token-order concatenation. So a panic during
    construction returned a **FULL-LENGTH, UNRECONCILED** result set — duplicated
    overwritten rows, resurrected deleted rows — as `Ok` with only a warning. The two
    are now distinguished at the TYPE level and only a reported construction failure is
    eligible for that fallback; a dead producer fails the read closed, with the panic
    message preserved.
  - An aborted/cancelled scan producer now surfaces as `Error::Cancelled` rather than
    the panic-flavoured internal error, so the reported cause matches what happened.

- **BTI `Rows.db` row-index base + leading `NEXT_COMPONENT` byte (#3002, #3040).** Two
  defects that cancelled each other on the BTI clustering read path are corrected:
  (a) the per-partition `TrieIndexEntry`'s SIGNED root delta is now measured from
  `RowsOffset + 2 + key_length` — the position AFTER the short-length-prefixed key,
  where Cassandra 5.0.8 captures `basePosition` — instead of a base 2 bytes low that
  dropped the root's own `ByteComparable.EMPTY` block-0 payload; and (b) the OSS50
  clustering-bound encoders now emit the `0x40 NEXT_COMPONENT` byte before EACH
  component INCLUDING the first, matching `ClusteringComparator`.
  - **Cassandra-written SSTables are unaffected by the change and now read
    correctly** — their row indexes were always canonical; only CQLite's
    reader/encoder were wrong.
  - **`Rows.db` row indexes WRITTEN by CQLite <= 0.16 are mis-rooted and must be
    rewritten** (re-flush or re-compact the affected tables, or regenerate the
    SSTables). Their entry deltas were encoded against the old 2-low base. Reads of
    such a file stay CORRECT but are affected in two different ways: a clustering
    `SELECT` silently loses the row-index narrowing (below), while the paths that
    REQUIRE a root — `iterate_rows_for_partition` and `sstable verify` — fail on it.
    `verify` records a `BtiTrieCorrupt` finding per affected partition — worded to say
    the file's bytes are INTACT and the remedy is a rewrite, not damage recovery — and
    drops that leaf from its cross-check set, so it also reports follow-on
    index-vs-Data.db findings for the same file; they all clear once the SSTable is
    rewritten.
  - A mis-rooted entry is now CHECKED structurally rather than trusted: a row-index
    root must precede its entry, be a payload-capable node type (and not the empty
    childless `PayloadOnly`-with-no-payload shape), and end exactly at the entry (the
    trie writer emits children before parents, so the root is the last node written).
    A rejected root makes the clustering read fall back to a **full-partition
    decode** — correct but unnarrowed — instead of returning a structurally valid but
    wrong window. `data_position`/`block_count` still decode, so point lookups and
    successor walks are unaffected. These conditions are **necessary, not
    sufficient**: a mis-based offset whose bytes happen to decode to a node ending
    exactly at the entry still validates and still narrows to a bogus window, so this
    is a safety net for the affected files, not a general detector — the remedy
    remains rewriting them.
  - New counter `cqlite.read.bti.rows_root_rejected` (`{partition}`, attribute
    `cqlite.read.rows_root_reject_reason`) makes that fallback visible instead of
    silent: it is 0 on a healthy table, and non-zero names the violated invariant
    behind otherwise unexplained clustering-read latency.

### Changed

- **BREAKING (`cqlite-core` public config): `Config.storage` is now the single
  source of truth for the write path, and its defaults are the values that were
  already running (#1697).** The public `Config` facade was decorative for the
  write path: the write engine ran off a separate, private `WriteEngineConfig`
  with its own independent literal defaults, so setting a public knob changed
  nothing, silently. One canonical bridge —
  `WriteEngineConfig::from_config(&Config, data_dir, wal_dir, schema)` — is now
  the only translation, and `WriteEngineConfig::new` is defined as that bridge
  applied to `Config::default()`, so every knob has exactly ONE literal default.
  All four write paths (core, CLI, Python, Node) route through it.
  - **`storage.memtable_size_threshold` default 16 MB -> 64 MB.** This does NOT
    change behaviour: 64 MB is the value that always actually ran (the engine's
    private default). Adopting the decorative 16 MB would instead have silently
    quadrupled everyone's flush rate. Code reading the default for its own sizing
    will see the new number.
  - **`Config::performance_optimized()` `memtable_size_threshold` 64 MB ->
    128 MB**, keeping the preset above the raised default so it still trades
    memory for throughput.
  - **New public fields** (all `#[serde(default)]`, so pre-existing config
    payloads and Python config dicts keep deserializing):
    `storage.memtable_hard_limit` (256 MB — the admission ceiling
    `check_admission` enforces; previously an invisible private constant that
    could hard-fail an embedder), `storage.compaction.min_threshold` (4) and
    `storage.compaction.max_threshold` (32) (STCS eligibility bar and merge-width
    cap; previously with no public counterpart at all).
  - **`Config::validate()` gained three rejection rules, so configs that
    previously validated can now be rejected** — each one a state that wedges or
    OOMs the write engine rather than merely being odd:
    `memtable_hard_limit > memtable_size_threshold` (a ceiling at or below the
    flush threshold rejects writes before a flush can relieve the memtable),
    `compaction.min_threshold > 0`, and
    `compaction.max_threshold >= min_threshold`. The memtable rule is STRICT —
    equality leaves no headroom, so an ordinary write is rejected at the ceiling
    while the memtable never reaches the flush trigger — while the compaction
    rule allows equality. Note the memtable rule is not a wedge-freedom
    guarantee: a single mutation larger than the headroom still wedges, which is
    an engine-side defect tracked as #3404. A fourth rule rejects a memtable byte count above the
    target's `usize::MAX`, reachable only on 32-bit/wasm32, where the value would
    otherwise land on `usize::MAX` and never flush AND never reject.
  - **Removed with no deprecation shim**: the four public constants
    `WriteEngineConfig::DEFAULT_FLUSH_THRESHOLD`, `DEFAULT_HARD_LIMIT`,
    `DEFAULT_COMPACTION_MIN_THRESHOLD` and `DEFAULT_COMPACTION_MAX_THRESHOLD`.
    Each was a second literal for a value that now lives once, in
    `Config::default()`; read `cqlite_core::Config::default().storage` instead.
  - **Removed**: `WriteEngineConfig::with_compaction_config`. It was a SECOND
    `CompactionConfig` -> engine translation with zero production callers, and
    nothing asserted the two agreed, so a knob threaded into `from_config` and
    missed there would have silently yielded engine defaults. Set
    `config.storage.compaction` and call `from_config`.
  - **CLI**: `CQLITE_MEMTABLE_FLUSH_THRESHOLD` is now applied to the public knob
    and VALIDATED. A value that wedges the engine (e.g. `300000000`, above the
    256 MB ceiling) is an error instead of being accepted in silence; a malformed
    value is an error instead of a silent fall back to the default (an
    empty/whitespace value still reads as unset). It is also parsed as `u64`, so
    a 32-bit host no longer silently ignores anything above `u32::MAX`.
  - **Python / Node bindings**: the `flush_threshold` / `flushThreshold` option is
    folded onto the public `storage.memtable_size_threshold` (so it OVERRIDES a
    value given in `config`) and its ceiling check now reads the CALLER's
    `memtable_hard_limit` rather than the default's. Raising the ceiling in
    `config` therefore raises what the option accepts; lowering it lowers it.

- **Behaviour change (`cqlite-flight`): `--max-concurrent-scans` now DEFAULTS to a
  core-derived value, `clamp(2 × P, 2, 64)`, instead of the constant 64 (#3225).**
  `P` is `std::thread::available_parallelism()` — the hardware threads available to
  **this process**, so the CPU affinity mask and the cgroup v1/v2 CPU quota are both
  honoured and a container limited to 1 CPU on a 96-core node no longer derives from
  96. **64 is retained as the CEILING** (#2420's blocking-pool/fd bound), so the
  default is unchanged at `P ≥ 32` and no deployment is admitted more widely than
  before; the floor is 2.
  - **Why: the old constant was measured suboptimal at every server width.** #3225's
    sweep (5 widths × ramp `1,2,4,8,16,24,32,64` × 3 reps, 126 points —
    `docs/reports/ws0-3225-report.md`) puts the throughput-optimal concurrent-scan
    count at **2 / 8 / 12 / 16 / 24** for 1 / 2 / 3 / 4 / 6 physical cores, and the
    constant 64 costs **−21.5%** throughput at one core through **−7.3%** at six, with
    per-scan p50 inflated **41.95× → 2.94×**. #3217's earlier "peak 16 at six cores"
    was a **censoring artifact** of a ramp that stopped at 16; the true peak is 24, and
    the derived default beats both N=16 (+8.0%) and the old N=64 (+7.9%) there.
  - **Provenance is logged, so the change is backwards-observable.** The existing
    `cqlite-flight starting` event gains `max_concurrent_scans_source`
    (`flag` | `env` | `derived` | `derived-fallback`) and `available_parallelism`
    (omitted when the oracle cannot answer — that case is `derived-fallback`, resolves
    to 64, and is never reported as `derived`).
  - **Restoring the previous behaviour is one flag:** `--max-concurrent-scans 64` (or
    `CQLITE_MAX_CONCURRENT_SCANS=64`) reproduces the pre-#3225 ceiling exactly on any
    host. Precedence is unchanged: flag → env → derived, and an explicit value is never
    clamped toward the derived one.
  - **Two residuals are documented in `cqlite-flight/README.md`**: the formula is
    −5.0% against the measured peak at the narrowest width (an accepted
    minimax-regret choice — `available_parallelism` cannot distinguish one SMT core
    from two non-SMT cores), and the non-SMT case is an **unvalidated extrapolation**
    (logical == physical there, so it admits 2 per physical core rather than the fitted
    4; no non-SMT host has ever been measured).

- **Breaking (API):** `storage::write_engine::build_single_partition_merger_from_readers`
  takes a required 5th argument,
  `PointAccessRecording::{Record, CallerRecords}`, selecting whether THAT call records
  the partition access for the #2827 access-distribution probe. Migration: pass
  `PointAccessRecording::CallerRecords` when an enclosing layer already records the
  logical read at its own boundary (what the core executor's multi-generation targeted
  read does, via `generation_merge`), or `PointAccessRecording::Record` when the call
  IS the logical point-read boundary (what the Flight warm point path does). Rows,
  ordering and probe behaviour are otherwise unchanged, and the argument is inert
  unless the default-OFF probe is enabled.
  - **No defaulted compatibility wrapper is provided, deliberately.** The two in-tree
    callers pass OPPOSITE values by design, so any default would be wrong for one of
    them — silently double-counting a logical read on one path or dropping it on the
    other. Both failures corrupt the histogram the probe exists to produce, and a
    dropped or duplicated access biases the derived cache verdict. Making the choice
    explicit at the call site is the whole point of the parameter, so a 4-argument
    wrapper would reintroduce exactly the hazard it removes (#2827).
- `BtiRowIndexHeader::trie_root` is now
  `Result<ValidatedRowsTrieRoot, RowsTrieRootRejection>` instead of a bare `usize`, so
  an unvalidated row-index root cannot be traversed by accident; use
  `trie_root_offset()` / `require_trie_root()` (#3002).
- **Breaking (API):** the batched streaming scan now returns
  `sstable::reader::BatchedScanStream` instead of
  `tokio::sync::mpsc::Receiver<Result<Vec<(RowKey, ScanRow)>>>` — affecting
  `StorageEngine::scan_stream_batched`, `SSTableManager::scan_stream_batched` (both
  feature variants) and `SSTableReader::scan_stream_batched`/`_admitted`. The new type
  owns the scan task's `JoinHandle` so a dead task cannot masquerade as end-of-stream
  (#3106). Its `recv()` keeps the same `async fn(&mut self) -> Option<Result<..>>`
  shape, so the usual `while let Some(batch) = stream.recv().await` consumer compiles
  unchanged; code that named the `Receiver` type, stored it in a struct field, or
  called `Receiver`-specific methods (`try_recv`, `blocking_recv`, `close`) must be
  updated.
- **Breaking (API):** the PER-ROW streaming scan now returns
  `sstable::reader::RowScanStream` instead of
  `tokio::sync::mpsc::Receiver<Result<(RowKey, ScanRow)>>` — affecting
  `StorageEngine::scan_stream`, `SSTableManager::scan_stream` and
  `SSTableReader::scan_stream`/`_admitted`. Same reason and same migration as the
  batched change above (both are now aliases of one `JoinedStream<T>`, re-exported
  together with `ScanStreamItem`): `recv()` keeps its
  `async fn(&mut self) -> Option<Result<..>>` shape, so `while let Some(row) =
  stream.recv().await` compiles unchanged, while code that named the `Receiver` type,
  stored it in a field, or called `try_recv`/`blocking_recv`/`close` must be updated
  (#3124).

## [v0.16.1] - 2026-07-23

_The CommitLog release._ A patch on v0.16.0 that adds a second Cassandra on-disk
format — CommitLog segment files — alongside SSTables. No breaking changes; the new
module and CLI subcommand are purely additive.

### Added


- Cassandra 5.0 CommitLog reader: `cqlite_core::storage::commitlog` with
  `CommitLogReader::open` / `open_with_schemas` and a lazy streaming `MutationIter`
  (one record at a time, 128 MB segment cap) (#2389, PR #2797). Contributed by
  @rustyrazorblade.
- `read-commitlog` CLI subcommand (JSON and text output) alongside the SSTable
  commands (#2389, PR #2797).
- Appendix H of the SSTable definitive guide: CommitLog on-disk format documentation
  (#2389, PR #2797).

### Notes

- The reader decodes the version-gated descriptor header (Cassandra 5.0 commitlog
  version 7), CRC-framed sync sections with torn-tail tolerance, and schema-aware
  mutation/cell decode for the common insert path. Unmodeled constructs (clustering
  columns, static rows, collection/complex columns, deletions, range tombstones) are
  reported structurally rather than misdecoded; compressed and encrypted segments and
  version-8 segments (`storage_compatibility_mode: NONE/UPGRADING`) fail closed with a
  typed error. No-heuristics throughout.
- Scope is reader-only. The CommitLog writer (#2388), CDC tailing, encryption support,
  and query/Flight integration are out of scope for this pass. Hostile-file hardening
  follow-ups are tracked in #2838.

## [v0.16.0] - 2026-07-22

_Trino connector completeness & cancellation._ Closes two field-surfaced connector
gaps on the v0.15.0 latency/throughput base: collection columns now project through
Trino, and weight-balanced split fan-out ships with a root fix for a
`LIMIT`-cancellation hang.

### Added

- Typed collection columns through Trino: `list`/`set`/`map` (including
  `list<frozen<udt>>`) project as Trino `array`/`row`/`map` instead of being silently
  dropped; unmappable columns are surfaced loudly. Primitive element types decode
  fully; UDT element-value decode inside a collection remains tracked to #2349 (#2815,
  PR #2816).
- Weight-balanced split→pod assignment via K-way token-range sub-splitting
  (`cqlite.sub-splits-per-range`, default 4) with span-proportional `SplitWeight`,
  evening out the 2–4× per-pod CPU skew. Aggregate, pushed-`LIMIT`, and fully-bound
  point reads are exempted to K=1 (#2680, PR #2833).
- Plan-time split pruning for fully-bound partition keys — a point read prunes to the
  covering split instead of fanning out (#2679, PR #2774; #2806, PR #2810).
- UDT registry wired into both Flight read paths, cold and warm (#2349, PR #2761).
- Keyspace-qualified UDT type names accepted on both read paths (#2807, PR #2808).
- BTI (`da`) and compressed-chunk-stitching end-to-end Flight `do_get` coverage (#2372,
  PR #2768; #2373, PR #2780); fine-grained `do_get` abort taxonomy and abort-path trace
  (#2681, PR #2784); `tables_discovered` / `warm_tables` gauges (#2684, PR #2786).

### Fixed

- **P0 `LIMIT`-cancellation hang fixed at root**: a partial-predicate `LIMIT` under
  sub-splitting could hang because the blocking Flight `DoGet` read ran on the Trino
  driver thread, so operator close could never cancel it. The read now runs off the
  driver thread (`isBlocked()`), letting close cross-cancel the stream; the server
  egress path also races a cancel flag. Guarded by a docker-compose E2E `LIMIT`
  regression test (#2782, PR #2833).
- Read-time TTL/liveness reconciliation routed through `do_get` and validated by the
  query-semantics oracle (#2374, #2789, PR #2800).

### Changed

- Recurring roborev blocker classes mechanized as `--lite` gate lints (#2656, PR
  #2741).

## [v0.15.0] - 2026-07-17

_Trino latency, throughput & operations_ (epic #2403). Turns the field-validated read
path into a fast, observable, overload-resilient one: warm throughput through Trino is
up roughly **15×** versus the v0.14 field baseline (round-11b measured ~34 qps warm,
p50 227ms / p99 366ms, server-side ~2ms, zero cold parses, at 80 threads with no
OOMKills).

### Added

- Flight `do_get` admission control: bounded scan concurrency
  (`--max-concurrent-scans`), `UNAVAILABLE` load-shedding, and phase-visible queueing;
  the eager multi-generation merge is admitted through the same semaphore (#2420, PR
  #2431; #2063, PR #2568).
- Five in-process saturation gauges — blocking-task guard, merge egress depth, and an
  fd/thread/RSS sampler on a 2s tick (#2419, PR #2547).
- Operator-facing flight-metrics reference and a refreshed cqlite-flight Grafana
  dashboard with a catalog-drift guard; `cqlite.errors.total` eagerly registered at 0
  on startup (#2426, #2427, #2288).
- Multi-node read fan-out: split primaries rotate across replica owners, so reads under
  RF=N are no longer pinned to a single pod (#2397, PR #2409).
- `tools/flight-loadgen` throughput harness (#2313, PR #2575); a `CQLITE_READ_PATH`
  forcing knob plus point-vs-full differential lane (#1918); a public Performance page
  with measured round-11b results (#2473, PR #2475).

### Fixed

- **P0 — silent row loss on large single-cell values**: rows with a single ≥~1MB cell
  were silently dropped because a 1MB `row_size` heuristic rejected them as corrupt. It
  is replaced with an authoritative remaining-bytes bound, per the no-heuristics mandate
  (#2436, PR #2482).
- v5 cell parsing hardening: overflow-safe cell bounds, `Float32`/varint parity, and a
  varint arm that decodes a CQL varint as `Value::Varint` rather than a blob (#1795,
  #1884, #1885, PR #2467, PR #2466).
- `GROUP BY` float/double groups by the Cassandra comparator: NaN → one group, `±0.0`
  distinct (#2074, PR #2488).
- `CompressionInfo` fail-closed: `max_compressed_length == 0` is rejected at parse and
  in the compressed-offset-window read instead of producing garbage (#2529, #2524);
  compressed `CHUNK_READ_CALLS` accounting restored (#2167).
- Complex-cell element TTL clamped to `i32::MAX`, matching the scalar reader (#2173);
  snapshot-aware SSTable identity parsing handles ID-ful snapshots (#2384).

### Changed / Performance

- Connector snapshot-lifecycle closure — per-`(keyspace, table)` reader reuse plus warm
  rebind (#2356, #2306, PR #2425); snapshot-retirement hardening with a background
  grace-sweep (#2452, PR #2579). Trino connector advanced to `0.14.3` / `0.14.4`.
- Lazy Summary-guided BIG index — `O(summary)` open, bounded point-lookup intervals,
  and token-pushdown scans (#2412, #2413, PR #2440).
- Row-granular point-read streaming — point reads and cache-warm merges drive the merge
  row by row instead of materializing (#2423, PR #2434); the read-path merge streams
  per-row via `StreamingMerger` (#2230).
- Zero-copy `Bytes`-backed `Value` on the read path (#1644, PR #2598); binary-search
  range shadowing in the merge core (#1669); a `MADV_RANDOM` point-read mmap (#2210) and
  a global bounded key→partition-offset cache (#2059).
- Uncompressed-SSTable compaction peak heap bounded from **410 → 54 MiB** via
  row-granular streaming (#2299, PR #2421).

## [v0.14.1] - 2026-07-13

_Cold-start parse fix._ A patch on v0.14.0 that fixes the cold first-query-per-table
parse cost v0.14.0 called out as a known limitation. No API or behavior changes — a
pure performance fix.

### Performance

- Retired the redundant `SSTableIndex` from BIG reader open: `SSTableReader::open` no
  longer builds a second in-memory index, so `Index.db` is parsed exactly once per open
  (2 → 1), and the surviving build uses capacity hints (linearithmic, not quadratic)
  (#2385, #2395, PR #2402).
  - 200k-entry index build: 6.17s → 0.061s (~100×).
  - Growth ratio (build time vs entry count): 15.5 → 4.96.
  - Resident index memory: roughly halved per generation.

## [v0.14.0] - 2026-07-13

_Flight field-readiness._ The Arrow Flight server and Trino connector read path are now
field-validated against a live, at-scale Cassandra deployment (round-9 field build;
validation tracker #2367).

### Added

- Streaming `do_get` scan: the non-stitching scan path no longer materializes the whole
  SSTable before the first emit — it walks the index lazily, applies `LIMIT`
  effectively, and tears producers down on cancel via a Drop-join (#2361).
- Resolve-phase parse-once warm registry: single-flight parse, rebind-by-inode, and
  cancel-aware parse remove the CPU spin that hung `LIMIT`, `count(*)`, and point reads
  at multi-million-partition scale (#2383).
- `do_get` pushes PK-equality predicates toward partition point-read / prune instead of
  a full merge scan (#2207).
- Published Arrow Flight server + Trino connector user-docs page (#2115).

### Changed

- **BREAKING (config schema):** `MemoryConfig` collapsed to a single real caching knob —
  `block_cache.max_size`, wired as the shared decompressed-chunk cache's byte budget
  (Epic B / B2, #1568). The decorative `MemoryConfig.row_cache`, `MemoryConfig.query_cache`,
  and `MemoryConfig.allocator` fields and the never-selected `CachePolicy::Lfu` /
  `CachePolicy::Arc` variants (all wired to nothing at runtime) were removed. A config
  that still names any removed field or variant now **fails closed** on deserialization
  (`deny_unknown_fields` / unknown enum variant) rather than being silently ignored.
  `Database::stats().memory_stats` keeps its shape but its block-cache numbers are now
  the real cache's hits/misses/occupancy (a repeated cached read yields a non-zero
  `block_cache_hit_rate()` instead of a structural `0.0`).
- **Reader open path (perf):** `CompressionInfo.db` is now parsed **exactly once** per
  reader open (was twice — a legacy `parse_binary` plus the modern `parse`), and the
  compression algorithm is derived from that single authoritative parse. The legacy
  `detect_and_initialize_compression` path — which also issued ~25 speculative
  `exists()` generation-probe stats per open — is deleted; the component name is derived
  deterministically from `SsTableDescriptor` (Epic G / G1, #1597). No read result
  changes (byte-for-byte parity preserved).

### Removed

- **BREAKING (public API):** deleted the dead SSTable reader stacks flagged by the
  read-path audit (Epic G / G1, #1597). Removed public items:
  `cqlite_core::storage::sstable::SchemaAwareReader` (and the `schema_aware_reader`
  module); the `chunked_data_reader` module and `ChunkedDataReader`;
  `compression::StreamingDecompressor`, `ChunkedDecompressionConfig`, and the duplicate
  legacy `compression::CompressionInfo` / `ChunkInfo` parser (`parse` / `parse_binary`);
  and the streaming half of `CompressionReader` (`read`, `read_streaming`,
  `with_block_size`, `block_size`) — `CompressionReader` is now a plain `{ algorithm }`
  field with `new()` + `algorithm()`. All were constructed only in tests or had zero
  production consumers.

### Fixed

- Warm-handles `ENOENT`: streaming merge producers no longer fail when a snapshot path
  goes stale after `clearSnapshot` — a path-liveness gate re-opens by live path (#2352).
- `do_get` snapshot-index reload/glob loop honors cancellation, so `LIMIT` queries no
  longer hang and the in-flight gauge no longer sticks (#2264, #2157).
- Flight producer `entry_to_row` no longer collapses multi-cell / collection columns via
  HashMap overwrite (#2324).
- Read-time reconciliation: a multi-generation `SELECT` applies read-time TTL /
  partition / range-tombstone visibility (#1849); `scan_stream` single-generation path
  returns rows on CQLite-written SSTables (#1897).
- Write path honors `USING TTL` and per-cell expiration; surviving live TTL cells stay
  byte-identical after compaction, including complex / collection / UDT elements (#1743,
  #2038, #1538, #1537); non-frozen collection round-trip fixed (#2035).
- Float/double ordering matches Cassandra (NaN last, `-0.0 < +0.0`) across `Value`
  comparison, `ORDER BY`, `MIN`/`MAX` (#2010, #1870).
- Point-lookup `WHERE pk = ?` returns typed columns and routes to the fast path instead
  of the legacy column-less heuristic fork (#2066, #1802, #1750).
- No-heuristics + parser hardening: blob decode no longer guesses on a hardcoded byte
  pattern; recursion-depth guards, `duration` `try_from`, clamped capacities; typed
  Zstd-dictionary rejection; checked Arrow collection offsets (#1630, #1414, #1723,
  #1488, #1486).
- Query-semantics oracle added so read-reconciliation bugs no longer pass physical-dump
  parity green (#1742); compaction finalize path fsyncs directories (#1959).

### Performance

- Read/parse/export hot paths from the audit epics (B–G, J–M, AC–AE): boxed `Value`
  variants (88B → ≤40B), byte-bounded result budgets replacing the 1M row-count cliff,
  `LIMIT`/`OFFSET` pushed into the scan, streaming multi-generation merge + streaming
  O(1) aggregates, a key→partition-offset cache, one-walk zero-copy BTI trie descent, a
  single read-side VInt decoder, and capacity-hinted Arrow builders (#1583, #1582,
  #1577, #1585, #1495, #1817).

### CI / testing

- Metadata-driven, feature-aware CI lanes replace the hand-maintained hardcoded test
  lists; first green `main` since 2026-07-06 (#2359, #2039). Dataset skip-guard checks
  `Data.db` so dataset-dependent tests skip rather than panic (#2065). De-flaked
  wall-clock / global-counter / RSS-monotonic tests (#1776, #1774, #1539).

## [v0.13.0] - 2026-07-05

_The performance release._ Read-path constant-factor wins (Epic E, C2), Node
bindings throughput, and byte-bounded result budgets, on top of v0.12.0's
byte-for-byte compaction parity — plus no-heuristics correctness fixes.

### Known Issues

- **Node.js `Test (windows-latest)` CI leg** has 3 chronic, windows-only
  test-suite failures (refresh generation-drop, write-smoke, execute-deprecation),
  tracked in [#1979](https://github.com/pmcfadin/cqlite/issues/1979). Windows Node.js
  CI is **best-effort, not a release gate**, so these were **waived** for the
  v0.13.0 release (waiver tracked in [#2007](https://github.com/pmcfadin/cqlite/issues/2007)).
  Linux and macOS Node.js CI are green and gating.
- **Coverage Gate `(enforced)` (90%) leg** is not green and is **waived** for the
  v0.13.0 release; restoration is post-0.13 — tracked in [#2022](https://github.com/pmcfadin/cqlite/issues/2022).

### Added

- Typed inner-UDT decode for frozen-UDT elements inside frozen collections (#1340, PR #1960)
- RF-correct logical-optimizer row-count estimate for the Flight/Trino connector (#1336, PR #1954)
- Byte-bounded result budget: `Error::ResultTooLarge` and `QueryConfig.max_result_bytes` (default 64 MiB) (#1582, PR #1890)
- Per-surface SSTable freshness contract plus explicit `Database::refresh()` (#1749, PR #1761)
- Compaction drops fully-expired SSTables whole via metadata only, overlap-safe (#1388, PR #1740)
- fsync the data directory before WAL truncate for SSTable durability (#1392, PR #1421)
- Intern per-cell column names as `Arc<str>` to cut row-decode allocations (#1334, PR #1533)
- Recursive comparator for composite collection element/key ordering (#1296, PR #1317)
- Preserve repaired metadata through compaction (Cassandra 5.0) (#1021, PR #1250)

### Changed

- **BREAKING (Python bindings):** CQL `duration` and `time` now decode to exact,
  lossless Python types, matching the Node binding (#1450). See the
  [v0.13 Migration Guide](docs/development/v0.13-migration-guide.md) for
  before/after examples. The previous mapping (M4 §5.2) was lossy and disagreed
  with Node, the CLI, and Cassandra:
  - `time` → `int` (nanoseconds since midnight), was `datetime.time` (which
    truncated sub-microsecond nanoseconds).
  - `duration` → `cqlite.Duration(months, days, nanos)`, was `datetime.timedelta`
    (which approximated months as 30 days and truncated nanoseconds to
    microseconds).

  Migration for code that relied on the old types (the
  [v0.13 Migration Guide](docs/development/v0.13-migration-guide.md) expands on
  this):
  ```python
  # OLD: t was a datetime.time; d was a datetime.timedelta
  # NEW: t is an int (nanoseconds); d is a cqlite.Duration
  import datetime
  t_ns = row["work_time"]                       # e.g. 3723123456789
  t = datetime.time(                            # reconstruct a datetime.time (µs, lossy)
      t_ns // 3_600_000_000_000,
      (t_ns // 60_000_000_000) % 60,
      (t_ns // 1_000_000_000) % 60,
      (t_ns // 1000) % 1_000_000,
  )
  d = row["duration_val"]                        # cqlite.Duration
  d.months, d.days, d.nanos                      # exact components
  ```
- **BREAKING (discovery-driven flows):** loading the schema for a table that was
  never registered or discovered now returns an error instead of a fabricated
  `uuid id` default schema. `SchemaManager::load_schema` previously invented a
  hardcoded schema for unknown tables, so queries against an undefined table
  returned fabricated-shape rows rather than failing — a no-heuristics violation.
  Unknown tables now fail honestly with `Table schema not found: {table_name}`,
  mirroring the I3 hard-fail precedent
  (#1626). Tables with real registered/discovered schemas (including all corpus
  parity tables) are unaffected (#1710). See the
  [v0.13 Migration Guide](docs/development/v0.13-migration-guide.md) for the
  per-surface error (Rust/Python/Node/CLI) and how to register a schema.

### Removed

- **BREAKING (CLI):** dropped YAML as a query output format. `--out yaml` and
  `--format yaml` are no longer accepted — both are now rejected at parse time
  with a clear error listing the supported values (`table`, `json`, `csv`,
  `parquet` for `--out`). YAML output was never implemented (it was only an
  unused `OutputMode`/`OutputFormat` variant), so this removes a dead surface
  rather than working behavior; anyone scripting `--out yaml`/`--format yaml`
  must switch to a supported format. A parse-rejection regression test guards
  against the variant being silently re-added (#283). See the
  [v0.13 Migration Guide](docs/development/v0.13-migration-guide.md).

### Fixed

- **No-heuristics: removed blob-decode byte-pattern guessing.** The raw-decode
  path no longer infers a value's type from its byte pattern; blobs that happened
  to look like other types are returned faithfully as blobs, per the no-heuristics
  mandate (#1630).
- **Unknown-table reads fail honestly** rather than returning a fabricated
  `uuid id` default schema (also listed under Changed as a breaking behavior;
  #1710).
- **BTI `export-sstable` partition count** is now read from the authoritative
  `Statistics.db` reader instead of a derived estimate (#1622).
- **Deterministic raw-only Snappy decode** with transient-only retry — removes a
  nondeterministic decode fallback on the raw path (#1588).
- **Full-scan no longer issues duplicate scans.** Retired
  `execute_parallel_table_scan` in favor of bounded streaming, eliminating the
  4× duplicate table scan on the full-scan path (#1691).
- Pre-admission memtable size check with a bounded iterative estimator (#1625, PR #1957)
- Grouped/filtered non-star aggregates now read the input columns they were missing (#1952, PR #1971)
- Parser hardening bundle: recursion-depth guards, duration i32 bounds, collection capacity limits (#1632, PR #1970)
- Aggregate/grouped result metadata names now match row value keys (#1763, PR #1953)
- Data-safe logging: log shapes not values — removed eprintln/WHERE-literal logging that could leak data (#1694, PR #1958)
- Surface chunk-CRC corruption on point lookups instead of silently proceeding (#1411, PR #1777)

### Performance

- **Read-path constant-factor bundle (Epic E):** query-engine hot-path cleanups
  (schema `Arc`, single projection, cached sort keys, plan cache, GROUP BY hash;
  #1587, PR #1867), read-path idiom bundle (de-async, `FxHash`, OFFSET skip/take,
  per-partition digest, IN-expansion allocation cuts; #1590, PR #1877), and dropping the
  `table_readers` read guard before scanning (#1591, PR #1882).
- **Point-read I/O (C2):** a `ReadAt` positional-read trait removes the point-read
  cursor convoy and the per-lookup `open(2)` (#1573).
- **Node bindings throughput:** batch-fetch streaming rows per async task (#1443),
  move (not deep-clone) row values in `executeNative` (#1447), and cache `Set`/`Map`
  constructors per result conversion (#1448).
- Cache derived comparators on `SchemaEntry` (#1709, PR #1963)
- One payload+CRC read per compressed chunk (E3 A5 read-path bundle) (#1585, PR #1955)

## [v0.12.0] - 2026-06-22

The compaction release. CQLite now rewrites and compacts Cassandra 5.0 SSTables
with **byte-for-byte parity against Apache Cassandra**, verified by a differential
harness that compacts the same inputs with both engines and compares the output
bytes plus an end-to-end Cassandra readback (Epic #842 → #921 → #938). Reaching
parity required modelling compaction the way Cassandra does — per-element/per-cell
merge reconciliation (#886, #899) rather than whole-cell/row-timestamp granularity —
and then implementing the full reconciliation rule set: complex-deletion
strict-supersede and shadow-before-purge (#887), tombstone-vs-expiring tie-breaks
(#848), `gc_grace`/`gcBefore` purging with overlap-aware partial-compaction safety
(#845, #935), range-tombstone shadowing (#846), per-cell and dropped-column purging
(#922, #847), row-deletion/live-cell coexistence (#932), row-tombstone
`localDeletionTime` preservation (#873), clustering identity through row tombstones
(#912), non-frozen UDT multi-cell read+write (#927, #929), and static-row presence
read from input headers (#850).

Beyond compaction, this release adds an **Arrow Flight server + Trino connector**
for querying SSTables as a federated source with predicate, token-range, and
aggregation pushdown (Epics #874, #918); **canonical BTI (`da`) write support** that
emits Cassandra-format trie-indexed SSTables and end-to-end BTI read (Epics #872,
#835); a **CDC-style delta-scan / delta-export** path that projects SSTable
generations to Parquet envelopes with full tombstone fidelity (Epic #696);
**`WRITETIME()` / `TTL()` in `SELECT`** (Epic #689); broad **query-engine
completeness** (PER PARTITION LIMIT, static columns, clustering order, clustering-key
bounds, cross-generation LWW merge, partition-targeted lookups); and **read-path
performance** work (parallel single-reader scans, a size-aware direct-I/O disk
backend with configurable prefetch, streamed writers, promoted index, BTI seeks).
Plus crates.io OIDC trusted publishing, a Homebrew tap, and a hardened CI/validation
pipeline.

### Added

- **Byte-for-byte compaction parity vs Apache Cassandra** (Epics #842, #921, #938) —
  a `cqlite compact` command and a differential harness that compacts identical
  inputs with CQLite and Cassandra and diffs the resulting `Data.db` bytes, wired
  into CI alongside an E2E Cassandra readback (#854, #858, #936). The merge path was
  re-modelled to per-element/per-cell granularity (#886, #899), enabling the full
  reconciliation rule set: complex-deletion strict-supersede + shadow-before-purge
  (#887), tombstone-vs-expiring(TTL) tie-break (#848), `gc_grace`/`gcBefore` purging
  (#845) with overlap-aware partial-compaction purging via per-key
  `maxPurgeableTimestamp` (#935), range-tombstone merge shadowing (#846), per-cell
  dropped-column purging (#922, #847), row-deletion/live-cell coexistence (#932),
  multi-cell collection/UDT merge per cell-path (#844), non-frozen UDT multi-cell
  support (#927, #929), single schema-ordered emission for mixed complex writes
  (#930), row-tombstone `localDeletionTime` preservation (#873), clustering identity
  through row tombstones (#912), and static-row presence from input headers (#850).
  Rules are documented in `docs/compaction/byte-parity-rules.md`.

- **Arrow Flight server + Trino connector** (Epics #874, #918) — query Cassandra
  SSTables as a federated Trino source over Arrow Flight, with predicate pushdown,
  arbitrary nested predicates (OR/NOT), token-range SSTable pruning, and aggregation
  pushdown (count/sum/min/max/avg + GROUP BY) including integer `avg` and float
  min/max with Trino-exact NaN ordering (#836, #898, #919). GROUP BY pushdown has an
  operator-configurable gate for high-cardinality cases (#937).

- **Canonical BTI (`da`) write + read** (Epics #872, #835) — emit Cassandra-format
  `da` (BTI) SSTables with a Partitions.db/Rows.db trie, drop legacy
  Index.db/Summary.db, and validate against `sstabledump`/Cassandra readback (#914);
  end-to-end BTI full-scan read support (#897); Data.db offset extraction from BTI
  trie payloads for O(log n) seeks (#833); read-path completion for wide partitions
  and BTI (#867).

- **CDC-style delta-scan and `delta-export`** (Epic #696) — a `scan_delta` streaming
  API emitting `DeltaRecord`s (upserts, static upserts, row/range/partition
  tombstones) with an Arrow envelope schema derived from the table schema, a
  `DeltaParquetWriter`, and a CLI `delta-export` subcommand for projecting an SSTable
  generation to a CDC Parquet file (#869, #871, #870, #876, #877, #880, #879). Ships
  with a DuckDB reference-merge reconciliation guide (#878) and a parity harness
  (#881, #882).

- **`WRITETIME()` and `TTL()` in `SELECT`** (Epic #689) — parse, validate, thread
  per-cell writetime/TTL metadata from reader to query rows (opt-in), and evaluate
  the functions in projections, with coverage across JSON/CSV/Parquet and the
  bindings (#837, #838, #840, #855, #856).

- **Query-engine completeness** (Epic #756) — `PER PARTITION LIMIT` (#859),
  `indexes_used` in query-plan results (#861), static-column tracking in discovered
  schemas (#862), clustering-order (ASC/DESC) discovery from the Statistics.db header
  (#863), UDT-reference validation against the registry at load (#860), clustering-key
  inequality bounds (`>=`, `>`, `<`, `<=`) (#791), and a partition-targeted lookup
  for fully-constrained `WHERE pk = ?` instead of scanning every SSTable (#949).

- **Size-aware disk-access backend with direct I/O and configurable prefetch**
  (#964) — a new `Auto` selector sizes each `Data.db` file against system RAM:
  small files are memory-mapped (resident in the page cache for repeated scans),
  while files larger than a configurable fraction of RAM (default half) use **true
  direct I/O** (`O_DIRECT` on Linux, `F_NOCACHE` on macOS) so a single huge scan does
  not thrash the page cache. New `StorageConfig` fields — `disk_access_mode`
  (`auto`/`buffered`/`mmap`/`direct`), `direct_io_memory_fraction`, `prefetch`
  (`off`/`sequential`/`willneed`/`auto`), and `direct_io_prefetch_bytes` — plus
  `CQLITE_DISK_ACCESS_MODE` / `CQLITE_PREFETCH` env overrides. Prefetch is wired to
  both paths (`madvise(SEQUENTIAL/WILLNEED)` on mmap, an aligned read-ahead window on
  the direct path), and every non-buffered backend degrades gracefully to buffered
  I/O when the OS/filesystem refuses it (network mounts, `O_DIRECT`-hostile
  filesystems). All fields are serde-default for backward compatibility; the legacy
  `use_mmap` flag still forces mmap.

- **Read-path performance** (Epics #906, #751) — restored parallel reads on a single
  `SSTableReader` via per-scan positioned reads (#815) with a concurrent-scan scaling
  benchmark (#917); cursor/channel groundwork for a streaming K-way merge (#754); a
  Cassandra-correct streamed promoted index for wide partitions (#752); and Index.db
  entries streamed to disk instead of buffered (#753).

- **Distribution** — crates.io publishing for `cqlite-core` + `cqlite-cli` (#782)
  migrated to OIDC trusted publishing (#786), and a Homebrew tap for the CLI (#781).

### Changed

- Incremental streaming of the read/scan path instead of full materialization (#790).
- crates.io release publishing now runs on tag via OIDC rather than a stored token
  (#895); linux-gnu CLI binaries build under `cross` to lower the glibc floor (#864).
- Cross-generation `SELECT *` now performs an LWW merge with tombstone suppression
  across SSTable generations (#883), and `WRITETIME`/`TTL` metadata reconciles across
  generations (#885).

### Fixed

- **Write-path / Statistics.db fidelity** (Epic #796) — populate the
  `estimatedTombstoneDropTime` histogram (#797), compute final Statistics.db delta
  baselines before the write loop (#799), wire real `l0_count`/`total_written` from
  the WriteEngine (#800), and a `write_dir` file lock to prevent concurrent-write
  corruption (#798).
- **Bindings** — CLI emits `null` for tombstoned cells in JSON (#806); Python provides
  a hashable representation for `SET<FROZEN<UDT>>` (#804) and thread-safe schema init
  for concurrent queries (#805).
- **Test & CI trustworthiness** (Epic #795) — resolve `CQLITE_DATASETS_ROOT` to
  `sstables/` and unmask 7 silently-skipped Python failures (#773), harden
  `agent-gate.sh` to cover the Python bindings and all integration targets (#865),
  retire a TTL byte-scan flake for a structural assertion (#774), and build DuckDB
  from source so the DS11 reconciliation job links on ubuntu (#916).

### Contributors

Thanks to everyone who contributed to this release:

- **Patrick McFadin** ([@pmcfadin](https://github.com/pmcfadin)) — maintainer; the
  bulk of compaction parity, BTI write/read, delta-scan, query engine, read-path
  performance, write-path fidelity, CI, and release infrastructure.
- **Jon Haddad** ([@rustyrazorblade](https://github.com/rustyrazorblade)) — the Arrow
  Flight server + Trino connector (#836) and the compaction byte-parity foundations:
  the rule spec + parity-auditor agent (#854) and the `cqlite compact` differential
  harness vs Apache Cassandra (#858).

Development was AI-assisted: substantial portions were implemented and reviewed with
Claude Code under human direction and review. Reconciliation edge cases were validated
against the `rustyrazorblade/cassandra` compaction reference.

## [v0.11.0] - 2026-06-15

Minor release bundling everything merged since v0.10.0. New capability: a
first-class Parquet writer lifted into `cqlite-core` behind a `parquet` feature,
with `export_parquet` methods on the Python and Node bindings (Epic #682).
New read coverage: version-gated read behavior for the Cassandra 5.0 `oa` format
and graceful handling of the `da` (BTI) format (VG1/VG3/VG5/VG6/VG7), real BTI
node-type dispatch (#651), schema-typed query result columns (#770), and
higher-fidelity Parquet/Arrow type mapping (#771). Plus opt-in memory-mapped
reads (#589, **off by default**) with their follow-up hardening (#591) and a
bounded uncompressed-read allocation (#592). Correctness:
TEXT/composite partition-key reconstruction on the scan path (#586), a safe
compaction async-to-sync bridge (#587), writer temporal-delta and tombstone
serialization fixes (#645, #723), Summary.db offset-table encoding (#718), and
removal of the last parser heuristic (#650). Plus removal of three dead mmap
readers (#590) and a new documentation site.

(The `0.10.1` version that briefly appeared in the manifests was never tagged or
published; its prepared notes are folded into this release.)

### Added

- **Parquet writer lifted into `cqlite-core` behind a `parquet` feature**
  (Epic #682) — the Parquet/Arrow export engine now lives in
  `cqlite-core/src/export/parquet.rs` behind an optional `parquet` cargo feature,
  with the CLI's `cqlite-cli/src/output/parquet.rs` reduced to a thin wrapper over
  it (#685). The Python and Node bindings gain `export_parquet` /
  `exportParquet(query, path, { rowGroupSize, compression })` methods so callers
  can stream query results straight to a Parquet file. Golden-file coverage lives
  in `cqlite-cli/tests/parquet_golden_tests.rs`.

- **Version-gated read support for the Cassandra 5.0 `oa` format** (Issues
  #653, #655, #672) — `VersionGate`s are threaded through the read path (VG1)
  so format-specific behavior is selected from the SSTable version rather than
  guessed. The `oa` (5.0 BIG) read behavior lives behind five `oa`-only gates
  (VG3) and the query path is gated end-to-end (VG6, including a range-tombstone
  marker-skip fix); all six `oa` fixture tables pass `sstabledump` parity. The
  `da` (BTI) format has a routing foundation that returns a graceful
  *unsupported* error instead of misreading (VG5). Table identity in discovery
  is now keyed by `(keyspace, table)` (VG7, #680). New `oa`/`da` fixtures and
  goldens ship in the `datasets-v3` test set (#654).

- **Real BTI node-type dispatch in `RowsParser`** (Issue #647) — `parse_node_data`
  was a stub that always returned `PayloadOnly`, mislabeling Single/Sparse/Dense
  nodes as leaves and returning wrong results for any multi-node trie. A shared
  `parse_bti_node` now dispatches all 16 `TrieNode` ordinals (including the
  packed 12-bit pointer variants), used by both `PartitionsParser` and
  `RowsParser`.

- **Schema `CqlType` threaded into query-result `ColumnInfo`** (Issue #674) —
  result columns now carry their declared CQL type from the schema rather than
  an inferred one, enabling type-correct downstream conversions in bindings and
  exporters.

- **Higher-fidelity Parquet/Arrow type mapping** (Epic #673) — nested and
  high-precision CQL types are preserved on export instead of being flattened:
  collections map to Arrow `List`/`Map`, and high-precision types keep their
  precision rather than degrading to strings.

- **Opt-in memory-mapped I/O on the SSTable read path** (Issue #589) — the
  reader now sits on a `BlockSource` abstraction with two interchangeable
  backends: a portable `BufReader<File>` (default) and a read-only `memmap2`
  mapping. When enabled, files at or above `mmap_min_size_bytes` (4096) are
  served from the OS page cache with no per-block read syscall, mirroring
  Cassandra's `disk_access_mode: mmap`. **Opt-in and off by default**
  (`use_mmap: false`); buffered I/O remains the portable, safe default. Map
  failures degrade gracefully to buffered I/O. Enable only for immutable local
  SSTables — external mutation/truncation of a mapped file or some network
  filesystems can `SIGBUS`; the write-while-mapped guard and Windows
  delete/replace policy from #591 make this safe for the supported use case.

### Removed

- **Removed three dead mmap-based SSTable readers** (Issue #590) — deleted
  `SchemaAwareSSTableReader` (`storage/reader.rs`), `OptimizedSSTableReader`
  (`storage/sstable/optimized_reader.rs`), and `StreamingSSTableReader`
  (`storage/sstable/streaming_reader.rs`). They were never constructed outside
  benchmarks and carried divergent, misleading mmap/threshold logic. The single
  real read path is `SSTableReader` with the opt-in `BlockSource::Mapped` mapping
  (#589). Benchmark coverage was retained on the real reader.

### Fixed

- **Bounded the uncompressed/headerless read allocation** (Issue #592) — the
  uncompressed read path (`read_uncompressed_data_block` in
  `storage/sstable/reader/block_io.rs`) read the entire current-position-to-EOF
  range with a single `vec![0u8; remaining]`, zero-initializing and copying the
  whole data section into one heap `Vec`. With the opt-in memory map (#589) the
  bytes could be resident twice, breaking the <128MB memory target on large
  uncompressed SSTables. The read now streams through a reusable scratch buffer
  capped at `read_buffer_size` (shared helper `read_into_vec_capped`, the same
  shape the compressed large-block path already used), so the transient working
  set no longer scales with file size and the redundant zeroing is gone.
  Behavior is byte-identical and the `estimated_memory_usage` health metric is
  unaffected (it accounts for the block cache, not transient read buffers).
  Regression coverage: an instrumented-reader unit test asserts the scratch
  buffer stays capped for a block 64× its size, plus an end-to-end test over the
  `uncompressed_table` fixture (`issue_592_bounded_uncompressed_read.rs`).

- **mmap write-while-mapped guard + delete/publication policy** (Issue #591) —
  hardens the opt-in memory-mapped read path (#589, default OFF) against the
  compaction delete path. A memory map aliases a Data.db file's bytes for the
  reader's lifetime; deleting or truncating a mapped file can fault with `SIGBUS`
  on Unix or block deletion on Windows. The invariant is now enforced and tested:
  1. Compaction reads its inputs through **buffered I/O**, never a memory map
     (pinned explicitly in `KWayMerger`, independent of the global `use_mmap`
     setting), and drains them into memory before any delete — so the merger
     never holds a mapping over a file it removes.
  2. SSTable deletion removes **`TOC.txt` first** (the publication barrier), then
     the data components best-effort. The compaction candidate scan
     (`scan_data_files`) now skips any Data.db lacking a sibling TOC.txt, matching
     the read path. A component still pinned by a mapped reader on Windows
     therefore becomes an invisible orphan (reclaimed by the startup sweep)
     rather than a failed delete or a duplicate-row source.

  Regression coverage: an end-to-end test opens the inputs through a mmap-enabled
  `SSTableManager` and then compacts/deletes them
  (`issue_591_mmap_compaction_delete.rs`), plus unit tests for TOC-first deletion
  and the publication-barrier candidate scan. Constraints documented on
  `StorageConfig::use_mmap` and the write engine.

- **Compaction panicked when triggered from an async context** (Issue #587) —
  a high-severity panic shipped in v0.10.0. `WriteEngine::maintenance_step()` is
  synchronous but bridges to async I/O to read a merge's input SSTables. The
  bridge used `tokio::runtime::Handle::current().block_on(future)` whenever a
  runtime was already running on the calling thread, which panics with *"Cannot
  start a runtime from within a runtime"*. Because the bridge is only reached once
  a merge has input SSTables to read, STCS compaction worked in isolation but was
  **unreachable from any `#[tokio::main]`/async caller** — including the CLI's
  `maintenance` and `export-sstable --compact` subcommands (both run under
  `#[tokio::main]`).

  Fix: the shared async-to-sync bridge (`merge::block_on_async`, now also used by
  `flush_internal` and `finalize_merge_blocking`) detects an already-running
  runtime and offloads the future to a dedicated scoped thread with its own
  runtime, joining before returning. This is runtime-flavor-agnostic (works for
  both multi-thread and current-thread runtimes, unlike `block_in_place`) and
  preserves the synchronous public signature of `maintenance_step` that the CLI
  and Python bindings depend on. The Node binding already wrapped the call in
  `spawn_blocking` and was unaffected; the Python binding calls from outside any
  runtime and was likewise unaffected. Regression coverage drives
  `maintenance_step()` from inside both runtime flavors
  (`cqlite-core/tests/issue_587_compaction_async_bridge.rs`).

- **Partition-key column dropped / `WHERE` on TEXT PK returned 0 rows** (Issue #586) —
  a correctness regression shipped in v0.10.0. On the scan + residual-filter path
  (used for `WHERE` on a TEXT partition key, unlike the Index.db point-lookup path
  for UUID keys, #548/#553), partition-key columns are reconstructed from the raw
  row key. The reconstructor assumed a `u16` length prefix for *every* TEXT key,
  which is the composite-component framing, not the single-component layout (raw
  bytes). Consequences, both now fixed:
  1. A **single-component TEXT partition key** (`id text PRIMARY KEY`) failed to
     decode; the error was silently swallowed, so `SELECT *` was missing the PK
     column and `WHERE id = '<literal>'` returned 0 rows.
  2. A **composite partition key** decoded every column from the first component,
     so second+ PK columns got the wrong value and non-text components (e.g. a
     `date`) became debug strings.

  The scan path now decodes through the canonical, always-compiled
  `storage::partition_key_codec`, the exact codec the write engine's
  `PartitionKey::from_bytes` uses (single source of truth for both paths). A failed
  reconstruction is now logged via `log::warn!` instead of being swallowed, so this
  class of bug cannot ship invisibly again.

- **Writer temporal deltas now use unsigned VInt, not ZigZag** (Issue #644) —
  per Cassandra's `SerializationHeader`, every row-header temporal delta
  (timestamp, TTL, local-deletion-time) is written with unsigned VInt. The
  writer previously ZigZag-encoded these fields while the reader (fixed in #629)
  expected unsigned VInt, so every positive timestamp delta read back as roughly
  2× its real value. Corrected across all `data_writer.rs` row/cell/complex/range
  paths.

- **Correct tombstone serialization in the Data.db writer** (Issues #716, #717) —
  fixes four tombstone shapes that Cassandra 5.0.2 rejected or misread on
  `nodetool refresh` readback. Tombstone cells now set `HAS_EMPTY_VALUE` (without
  it the reader consumed a phantom value and desynced the row stream), and row
  tombstones now write the columns subset after the deletion times as
  `UnfilteredSerializer` requires (omitting it made Cassandra read the next row's
  flags byte as the subset bitmask). Pure row tombstones no longer carry
  primary-key liveness, matching Cassandra's serializer.

- **Correct Summary.db offset-table encoding and first/last key tracking**
  (Issue #666) — offset-table entries are now biased by the offset-table size so
  `offset[0]` equals the table size (absolute layout Cassandra's
  `IndexSummary.deserialize` asserts), and first/last key plus partition count
  are tracked for every partition via a new `note_partition()` rather than only
  at sampling boundaries — so tables with fewer than `min_index_interval`
  partitions no longer collapse the range filter to a single key.

- **Removed the last parser heuristic from `parse_cql_value`** (Issue #648) — the
  Ascii/Varchar arm carried three heuristic fallbacks (4-byte length prefix,
  null-terminated, raw UTF-8) "for test compatibility," violating the
  no-heuristics mandate (#28). The caller already extracts exactly the value
  bytes, so the entire slice is the text; the default path now treats it as UTF-8
  and errors on invalid input instead of silently accepting garbled data. The old
  paths remain behind the opt-in `legacy-heuristics` feature flag.

### Documentation

- **New documentation site** — a Starlight-based site (with rustdoc published to
  `/api/`) consolidates the user, CLI, bindings, use-case, and agent-developer
  docs, replacing the scattered in-repo guides as the source of truth.

- **SSTable definitive guide audited against Cassandra 5.0.8** — the Data.db,
  Index/Summary, Statistics.db, compression, bloom-filter/checksum, BTI, SAI,
  and version-matrix chapters were verified field-by-field against the
  cassandra-5.0.8 source and corrected.

## [v0.10.0] - 2026-06-02

Minor release. Three query-engine correctness/performance fixes (#548, #553,
#581), the new `Durability` write API (#547), `write-support` enabled by
default (#558), and a batch of developer-experience, CI, and documentation
improvements. 14 PRs since v0.9.2.

### Fixed

- **UUID/TIMEUUID WHERE clause returned 0 rows** (Issue #548) — `WHERE id = <uuid-literal>`
  now correctly returns the matching partition. Four bugs were fixed together:
  1. `QueryParser::parse_value` now recognises bare UUID literals (`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`)
     and produces `Value::Uuid([u8; 16])` instead of `Value::Text`.
  2. `QueryExecutor::value_to_row_key` now handles `Value::Uuid` (produces 16 raw bytes)
     and `Value::Tuple` (composite-PK framing `[len u16 BE][value][0x00]` per component,
     matching `PartitionKey::to_bytes`). Also adds `Value::BigInt` support.
  3. `QueryExecutor::compare_values` now has `(Value::Uuid, Value::Uuid)` arms for
     WHERE-clause filter evaluation in table-scan paths.
  4. `SSTableManager::get` now routes lookups through `table_readers` (keyed by
     unqualified table name) instead of `readers` (keyed by filename), which caused
     all SSTables to share a single HashMap entry and only the last-loaded one to be
     searched. This also fixes point lookups for all other types.

  Additionally, `SSTableReader::scan_for_key` now passes the reader's own schema to
  `stitch_and_parse_all_chunks` so V5CompressedLegacy rows parse during the scan
  fallback without an external schema.

- **Index.db point-lookup performance cliff** (Issue #553) — `lookup_partition_with_index`
  previously computed a Murmur3 digest of the raw partition key and looked that digest up in
  the Index.db `key_lookup` map, which is keyed on **raw** partition key bytes (since #552).
  The digest never matched, so every `get()` call fell back to an O(n) sequential scan of
  Data.db. Results were always correct but at O(file-size) cost per lookup.

  Fix: the digest computation (`compute_partition_key_digest`) has been removed from the
  hot path. `lookup_partition_with_index` now passes the raw `partition_key: &[u8]` bytes
  directly to `index_reader.lookup_partition`, restoring the O(1) HashMap lookup that was
  present before #552 changed the key representation. Callers already pass raw bytes
  (single = raw value bytes, composite = `[len u16 BE][value][0x00]` per component).

  `lookup_partition_with_schema_context` (the schema-driven variant) is unchanged.

- **`LIMIT` ignored on streaming `SELECT`** (#581): `Database::execute_streaming`
  yielded the entire result set regardless of `LIMIT`. The streaming producer
  (`execute_streaming_background`) only logged the `LIMIT` step and relied on a
  consumer that never enforced it, so `SELECT … LIMIT N` streamed every row — a
  silent wrong-result bug. The producer now enforces `LIMIT`/`OFFSET` inline
  during the scan (skip `OFFSET` matches, stop sending once `count` rows are
  emitted, and return so the scan stops early), matching the non-streaming
  `execute_limit` semantics. Regression test:
  `tests/test_issue_581_streaming_limit.rs`.

- **Provenance gate false-positives on branch names**: `scripts/ci/ensure_real_dataset.sh`
  now restricts its environment-variable scan to dataset-relevant names (`*_ROOT`,
  `*_PATH`, `DATASET*`) instead of scanning every env var. GitHub CI vars such as
  `GITHUB_HEAD_REF`, `GITHUB_REF*`, and `GITHUB_BASE_REF` are no longer inspected,
  so branch names containing words like "fixture" or "mock" no longer cause spurious
  gate failures. The `DATASET_SHA256` checksum check and CLI-argument scan are
  unchanged (#545).

### Added

- **Performance methodology doc** — new `docs/performance.md` (Issue #575)
  explains what the CI perf gate enforces (strict: `read/*`, `write/ingest_wal_off`,
  `write/flush`) versus what it tracks as advisory (`write/ingest_wal_on`), why
  CI absolute numbers are not authoritative for fsync-bound work, how to
  reproduce benchmarks locally with exact `cargo bench` invocations and the
  `Durability` knob, the effect of tmpfs vs disk on `ingest_wal_on` throughput,
  and a direct answer to "is ~282 ops/sec expected?" (yes — it is disk-bounded
  by per-write fsync latency at ~1 000 / fsync_ms ops/sec, not a cqlite
  regression). Linked from README Resources section.

- **WAL durability toggle on `WriteEngine`** — `WriteEngineConfig` now has a
  `durability` field (default `Durability::SyncEachWrite`) and a matching builder
  method `with_durability(Durability)`. When set to `Durability::Disabled`,
  `write()` and `write_async()` skip WAL append and fsync entirely, buffering
  mutations in the memtable only; data becomes durable only after a successful
  `flush()` or `close()`. Default behavior (`SyncEachWrite`) is **unchanged**: a
  successful `write` call still guarantees the mutation is durable on disk (#547).

  ```toml
  # Public API additions (cqlite-core::storage::write_engine)
  pub enum Durability { SyncEachWrite, Disabled }
  impl WriteEngineConfig { pub fn with_durability(self, Durability) -> Self }
  ```

  **Hazard note**: `ingest_wal_on` benchmarks may show fsync-latency noise on
  shared CI runners; this is expected and does not indicate a regression. Only
  `Durability::Disabled` paths are CPU-bound and gate-able.

- **`write/ingest_wal_off` benchmark** — new Criterion bench in
  `cqlite-core/benches/write.rs` that runs the same 256-row ingest loop as
  `ingest_wal_on` but with `Durability::Disabled` (#574). The measured path
  performs no `wal.append()` or `wal.sync()`, isolating pure CPU + memtable
  cost. This bench is strictly gated in the CI perf regression gate;
  `ingest_wal_on` is now classified as advisory (reported, never fails CI on
  its own). A new `open_write_engine_wal_off` fixture helper in
  `benches/fixtures/mod.rs` constructs the WAL-disabled engine.

- **Perf-gate redesign — strict vs advisory benches** (Issue #572). The CI
  performance regression gate now distinguishes two bench classes, driven
  entirely by `cqlite-core/benches/perf-gate.json`:

  - **Strict** (`read/*`, `write/ingest_wal_off`, `write/flush`): non-zero exit
    on regression beyond per-bench `threshold_pct` — these are CPU-bound with
    stable timings suitable for reliable regression detection.
  - **Advisory** (`write/ingest_wal_on`): delta reported in every CI run but
    **never causes a non-zero exit**, regardless of magnitude. `ingest_wal_on`
    is I/O-dominated by `fsync`; its wall-clock time varies well beyond 10% on
    shared GitHub-hosted runners, producing false-positive failures on PRs that
    cannot affect performance.

  Configuration: `perf-gate.json` now uses per-bench objects (`id`,
  `threshold_pct`) and an `advisory_benches` string list. The gate script
  (`scripts/ci/check_perf_regression.py`) is fully data-driven from this file —
  no bench names are hardcoded in the script. A suite of pytest fixtures in
  `scripts/ci/tests/` validates the strict-fail / advisory-pass behavior.

- **Gate workflow path filter** (Issue #572, Phase A). The
  `perf-regression.yml` workflow now uses a `paths` allowlist that excludes
  `docs/**`, `**/*.md`, `examples/**`, and other non-runtime `.github/**`
  files. Docs-only / examples-only PRs no longer trigger the benchmark gate,
  eliminating false-positive regression alerts from fsync noise on those PRs.

- **Linux x86_64 musl release target + SHA-256 checksums** — release binaries now
  include a statically-linked `x86_64-unknown-linux-musl` artifact plus a SHA-256
  checksum per asset, and the README gained an install section (#561, #568).

### Changed

- **`write-support` is now a default feature** of `cqlite-core`. The write path
  (`WriteEngine`, `Mutation`) is available out of the box; downstream consumers no
  longer need to opt in to enable it. This adds **no new dependencies** —
  `write-support` gates only first-party code, so the dependency surface for
  read-only consumers is unchanged. `flush`/`compact` on the high-level `Database`
  type remain behind the separate `experimental` feature (#558).

### Documentation

- **README feature → public-API table** mapping each Cargo feature to the API it
  gates (#557).
- **"Using cqlite-core as a dependency" guide** plus a compiling write-path example
  (#559).
- **Write-path concurrency & durability model** documented end to end (#560).
- **Per-tag rustdoc published to GitHub Pages** with a discoverable changelog link
  (#563).

## [v0.9.2] — Correctness fixes

Reader and compaction correctness follow-ups to v0.9.1, plus a compaction memory
fix and a multi-partition Index.db reader fix. No new features and no public API
changes.

### Fixed

- **`scan()` result ordering** is now guaranteed to be ascending Murmur3 token
  order (with raw key bytes as the equal-token tiebreaker), matching the on-disk
  SSTable layout and the write engine. Previously rows could come back out of
  order; `LIMIT` is now applied after ordering (#516).
- **`get()` / `scan()` partition consistency**: `get()` no longer returns `None`
  for partition keys that `scan()` returns. An Index.db digest-lookup miss now
  falls back to a key scan, and the V5CompressedLegacy chunk-stitching parse path
  is used so partitions spanning chunk boundaries are found (#517).
- **`SSTableReader::stats().block_count`** is now populated from the authoritative
  `CompressionInfo.db` chunk count instead of always reporting `0` (#518).
- **Compaction dropped input tombstones**: the k-way merger now surfaces row and
  cell tombstones from input SSTables with their authoritative `markedForDeleteAt`
  timestamps, so a higher-timestamp tombstone in a later SSTable correctly shadows
  a live row from an earlier one (#505).
- **Equal-timestamp Delete-vs-Live reconcile** now follows Cassandra
  `Cells#reconcile`: at equal timestamp the tombstone wins, independent of input
  file recency (previously the newer file won regardless of liveness) (#498).
- **Compaction dropped disjoint columns**: the k-way merger now reconciles cells
  per column (Cassandra `Cells#reconcile`) instead of selecting one whole winning
  row per clustering key, so rows updated across SSTables on different columns keep
  all their cells after compaction (#533).
- **Compaction memory**: the SSTable writer now streams `Data.db` to disk per
  partition instead of buffering the entire component in memory, bounding peak heap
  to roughly the largest single partition (was O(whole file), exceeding the 128 MB
  target on large compactions). Output is byte-identical (#492).
- **Multi-partition Index.db reader**: the reader mis-parsed Index.db entries whose
  leading `u16` key length was not `0x0010`, treating it as a digest marker and
  dropping most partitions (e.g. 100 partitions read back as 2). It now parses the
  real Cassandra BIG format `[key_len][raw key][offset][promoted]` for any key
  length; the project guide's Index.db documentation was corrected to match, and
  the `write-support` test targets are now wired into CI so this class of failure
  can't rot again (#552). Restoring O(1) raw-key point lookup is tracked in #553.

## [v0.9.1] — Reader correctness fixes

Reader correctness and test/CI follow-ups to v0.9.0. No writer changes and no
public API changes.

### Fixed

- **Set-element tombstones** were surfaced as live values by the V5CompressedLegacy
  parser because the cell `is_deleted` flag was discarded. `parse_complex_cell_value`
  now returns the deletion flag, and the set (and list) branch skips tombstoned
  elements (#493).
- **Schema-aware tuple decoding** for arbitrary arity: tuples with more than two
  elements (e.g. `tuple<int, text, uuid>`) previously read back as `Null` or `Blob`.
  The reader now decodes each element using the element types from the schema's
  type string, with bounds-checked parsing and no heuristics (#501).
- **Frozen UDT field decoding**: `frozen<NAME>` columns previously read back as
  `Frozen(Null)`. The reader now resolves the concrete UDT through the UDT registry
  and decodes fields by name and type, and returns an actionable error when the
  referenced UDT is not registered (#502).

### Testing & CI

- Revived the orphan root-package integration tests: hardcoded SSTable directory
  UUIDs (from the retired dataset version) were replaced with dynamic table
  discovery, and the suite is now wired into CI so it cannot rot again (#514).
- Fixed the `aarch64-apple-darwin` CI runner where `cargo` was routed to
  `rustup-init` (the `cargo metadata` / `cargo +1.88.0` failures). The real cargo
  is now prepended to `PATH` in the Node and Python build workflows, with a
  toolchain verification step (#512).

### Known Issues

- Reviving the orphan integration tests surfaced three pre-existing reader bugs
  (`scan()` ordering #516, `get()`/`scan()` consistency #517, and
  `stats().block_count` #518). All three are **resolved in v0.9.2**.
- The v0.9.0 known issues for counter writes, the BIG-format BTI writer, and the
  Python concurrent-query race (#311) are unchanged.

## [v0.9.0] — M5 Write Support

### Added

- **WriteEngine** in `cqlite-core/src/storage/write_engine/`: WAL-backed memtable,
  STCS compaction, and flush to portable Cassandra 5.0 SSTables. Public methods:
  `write(mutation)`, `write_async(mutation)`, `flush()`, `maintenance_step(budget)`,
  `maintenance_stats()`, and `export_sstable(path)`.
- **Mutation API** (parser-independent): `Mutation { table, partition_key,
  clustering_key, operations, timestamp_micros, ttl_seconds }` with
  `CellOperation::Write | WriteWithTtl | Delete | DeleteRow`.
- **CQL text write path**: `db.execute("INSERT/UPDATE/DELETE …")` as a convenience
  layer on top of the mutation API (PR #487).
- **Type coverage** for write roundtrips: Inet, Varint, Duration, Tuple, and
  Frozen all roundtrip through write→flush→read (Issue #477, #478).
- **Counter guard**: `WriteEngine::write()` and `write_async()` return
  `Error::InvalidOperation` immediately when a mutation targets a counter column,
  preventing silent data corruption (Issue #479, PR #489).
- **Python bindings write support** (PR #488): `db.execute(INSERT/UPDATE/DELETE)`,
  `db.flush_run()`, `db.maintenance_step(budget_ms)`, and `db.write_stats` property.
  Open database with `writable=True, write_dir=path` to enable writes.
- **Node.js bindings write support** (PR #494): `await db.execute(INSERT/UPDATE/DELETE)`,
  `await db.flushRun()`, `await db.maintenanceStep({ budgetMs })`, and
  `db.writeStats` getter. Open with `{ writable: true, writeDir: path }`.
- **CLI write flags**: `--writable`, `--write-dir`, `--mutation`, `--mutations-file`,
  `--flush`. Subcommands: `maintenance --budget-ms`, `write-stats`, `export-sstable`.
- **E2E readback gate** (`test-data/scripts/e2e-cassandra-readback.sh`, PR #508):
  exercises 5 tables (basic-primitives, collections, udt, static-columns, ttl)
  through write → flush → Docker copy → `nodetool refresh` → `cqlsh` verify.
- Write→flush→read roundtrip tests for `Inet`, `Varint`, and `Duration` types
  (Issue #477).
- Write→flush→read roundtrip tests for `Tuple<int, text, uuid>` and
  `Frozen<udt>` types (Issue #478).

### Changed

- M5 milestone closed; v0.9.0 marks the first release with full write support.
- CHANGELOG promoted from `[Unreleased]` to `[v0.9.0]`.

### Fixed

- Static columns could be duplicated in query results; fixed in PR #490
  (Issue #480, `static_columns_table` xfail removed).
- `typed_collections_table` V5CompressedLegacy cell extraction returned 1 row
  instead of 50; reader fallback added in PR #506 (Issue #481).
- Static-row write path emitted incorrect flags; fixed in PR #509.

### Known Issues

- **Counter writes**: Counter columns cannot be written via CQLite. The `write()`
  call returns `Error::InvalidOperation` with a descriptive message. Cassandra
  requires distributed CAS semantics for counter increments.
- **BTI writer**: The SSTable writer emits BIG format index files. BTI (trie)
  format indexes are read-only for now.
- **Python concurrent-query race** (Issue #311): Concurrent queries on the same
  database handle may see a race in schema metadata access. Run one warm-up query
  before spawning parallel threads.
- **Open reader follow-ups**: set-element tombstone decoding (#493), schema-aware
  tuple decoding (#501), frozen<udt> field decoding (#502). _(Resolved in v0.9.1.)_

## [0.4.0] - 2026-01-27 (M4 Complete)

### Added
- Python bindings via PyO3 with sync-first API (Issue #289)
- Node.js bindings via napi-rs with Promise-based API (Issue #290)
- Streaming API for memory-efficient large result sets (Issue #305)
- Complete CQL type coverage in bindings (20+ types including collections, UDTs)
- Type stubs for IDE support (Python mypy, TypeScript definitions)
- Thread-safe database handles with idempotent close
- pip/npm installable packages (5 platform builds each)
- 500+ tests across Python and Node.js bindings

### Python Bindings
- `cqlite.open()` context manager API
- `Database.execute()` for query execution
- `Row.to_dict()` for dictionary conversion
- `StreamingIterator` for large result sets
- Native Python types (datetime, UUID, bytes, Decimal)

### Node.js Bindings
- `Database.open()` with async/await pattern
- `Database.executeNative()` for native JS types (BigInt, Date, Buffer, Set, Map)
- `Database.executeStreaming()` for async iteration
- Complete TypeScript definitions with no `any` types
- Error properties: `code`, `category`, `isRecoverable`

## [0.3.0] - 2026-01-20 (M3 Complete)

### Added
- Parquet output format with Snappy compression (Issue #277)
- `cqlite export` command for file-based data export (Issue #278)
- Streaming export infrastructure for memory-efficient large dataset handling (Issue #280)
- Export formats: CSV, JSON, Parquet, CQL (INSERT statements)
- Progress bar with statistics for exports
- Atomic file writes to prevent partial output files (Issue #279)

### Changed
- Removed YAML from output format options (Issue #283)

## [0.2.0] - 2026-01-08 (M2 Complete)

### Added
- CLI one-shot query mode with `--schema`, `--data-dir`, `--query`, `--out` flags
- REPL mode with history, completion, and status display
- TUI mode (experimental)
- SELECT query support with WHERE clause (partition/clustering key equality)
- Output formats: Table, JSON, CSV
- M2SelectValidator for query validation

### Changed
- Query engine enabled by default (`state_machine` feature)
- Documentation updated for M2 completion

## [0.1.0] - 2025-12-18 (M1 Complete)

### Added
- Initial release of CQLite core library
- Cassandra 5.0 SSTable format support ('oa' format with BTI indexes)
- SSTable component parsing:
  - Data.db (row and cell data)
  - Index.db (partition index)
  - Summary.db (index summary)
  - Statistics.db (SSTable metadata)
  - TOC.txt (table of contents)
- Compression codec support:
  - LZ4
  - Snappy
  - Deflate
  - Zstd
- CQL type system implementation:
  - Primitive types (int, bigint, text, blob, uuid, timestamp, etc.)
  - Collection types (list, set, map)
  - User-defined types (UDT)
  - Frozen types
- Schema-aware decoding
- CLI tool with basic parsing commands
- Workspace structure:
  - `cqlite-core`: Core parsing library
  - `cqlite-cli`: Command-line interface
- 33/33 test tables passing (100% validation)

### Technical Details
- Zero-copy parsing where possible
- Memory-efficient design targeting <128MB for large files
- No external cluster dependencies required
- Real Cassandra SSTable test data validation

[v0.9.0]: https://github.com/pmcfadin/cqlite/compare/v0.4.0...v0.9.0
[0.4.0]: https://github.com/pmcfadin/cqlite/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/pmcfadin/cqlite/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/pmcfadin/cqlite/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/pmcfadin/cqlite/releases/tag/v0.1.0
