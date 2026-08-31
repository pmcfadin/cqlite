# Design: removing the `[[Set]]` channel from row and JSON-object construction

Issue #3630. Decision recorded in `proposal.md`: rows keep `Object.prototype` with own-property
definition; JSON-object cells become null-prototype. This document records **how**, and the two
decisions with real cost — the row write mechanism, and the JSON test subject.

## D1 — Which mechanism defines a row's own properties

All three candidates remove the channel (none performs a `[[Set]]` against an inherited accessor).
They differ in cost on the binding's hottest path and in how much `unsafe` they introduce.

| | Mechanism | Keeps #1446 interning | `unsafe` | FFI calls / row | Known risk |
|---|---|---|---|---|---|
| **M1** | napi-rs `JsObject::define_properties(&[Property])` | **No** — `Property` carries a `CString` `utf8name` and `Property::raw()` sets `name: null`, so the pre-interned `JsString` handles cannot be reused | none | 1 | one `CString` allocation **per column per row**, plus V8 re-interning each name per row — the exact `O(rows × columns)` cost #1446 removed |
| **M2** | raw `napi_define_properties` with a `Vec<napi_property_descriptor>` **prebuilt once per result** inside `ColumnKeys` (constant `name: <interned napi_value>`, `value` slot refilled per row) | **Yes, exactly** | one block, the first in `bindings/node` | 1 | introduces `napi::sys` + `unsafe` to a crate that has none today; descriptor lifetime must be tied to the `Env` scope the interned handles are valid in |
| **M3** | build on `Object.create(null)` (safe `set_property`, interned), then `Object.setPrototypeOf(row, Object.prototype)` | Yes | none | N + 1 JS call | V8 leaves fast mode on a prototype transition; the worst profile of the three, on the hottest path |

**Recommendation: implement M1 first, measure, and adopt M2 only if M1 regresses.** Reasons, in
order:

1. M1 is safe code and is a small diff. `bindings/node` has **zero** `unsafe` today (verified: no
   `unsafe`/`napi::sys`/`sys::` anywhere in `bindings/node/src/`), and spending that budget before
   measuring would be spending it on a prediction.
2. The regression M1 risks is **real and specific**, not hypothetical: #1446 exists because
   per-row re-interning is measurable on a wide-table scan. So M1 ships only with a measurement.
   The subject is a wide-table scan through the Node binding (the `test_wide_rows` keyspace), rows/s
   before vs after, reported in the PR body. **A regression outside noise selects M2**, which
   preserves #1446 by construction and is *fewer* FFI calls per row than today's N `set_property`
   calls — plausibly a speedup.
3. M3 is dominated: it keeps interning like M2 but adds a prototype transition V8 is documented to
   punish, and it is the only option whose object is briefly in a state (null prototype) that a
   concurrent consumer could observe if the construction ever became reentrant. Recorded and
   rejected, not deferred.

**One behavioural difference M1 carries and M2 does not:** `Property::new(name)` builds a `CString`,
which **fails on an interior NUL byte** in a column name. That is a fail-closed error rather than
silent loss, which is the right direction — but it is a new refusal on a name Cassandra could in
principle carry, and it must be an `Err` through the one FFI error contract (`to_napi_error`), never
a skipped column. M2 has no such restriction (a `napi_value` name is length-delimited). If M1 is
adopted, this refusal is a stated part of the contract; the spec covers it.

**Applies to BOTH row paths.** The interned path and the extras path are one requirement, not two:
#3504 found the same duplication one file over, and a fix that reaches only the interned path leaves
every aggregate-keyed and empty-metadata result still losing the column. The extras path has no
interned handles at all (names come from the value map), so under M2 its descriptors are built
per-row from `env.create_string(name)` — cheap, because the extras path is already the rare branch
and already sorts.

## D2 — The row test subject: extend the #3504 fixture generator

The oracle must be **Cassandra-written**, on the #3504 precedent, and there is a generator to extend:
`test-data/scripts/generate-issue-3504-udt-collision.sh` (cassandra:5.0.2 container, `cqlsh` INSERT
+ `nodetool flush`). Docker is available on the worker box (verified).

The new subject is a table whose **row columns** are quoted identifiers colliding with
`Object.prototype`:

```cql
CREATE TABLE row_collide (
    id           INT PRIMARY KEY,
    "__proto__"  text,
    "constructor" text,
    "toString"   text,
    real_col     int
) WITH compression = {'enabled': 'false'};
```

Three rows: one with all collision columns non-null (the string case), one with `"__proto__"` NULL
(the prototype-replacement case), one with none of them set (the contrast case). `USING TIMESTAMP
1000` on every INSERT so the committed JSONL golden is reproducible, matching the #3504 fixture's
convention.

`"constructor"` and `"toString"` are **not decoration** — they are what distinguishes *removing the
channel* from *renaming the delimiter* (issue AC7). A literal-`__proto__` special case passes the
`__proto__` cases and fails these; and unlike `__proto__` these two are **plain inherited data
properties, not accessors**, so before the fix they behave differently again (a `[[Set]]` on a
writable inherited data property *does* create an own property, so they are the cases that must be
asserted to still work, i.e. proof the fix did not regress the ordinary path).

**Whether a new fixture is needed at all is a measurement, not an assumption.** `SELECT id AS
__proto__` would need no fixture if CQLite's query surface supports aliases; the implementer
determines that first, and if it does, the alias case is an ADDITIONAL subject (it is the user-facing
reproduction the issue names) — not a substitute for the committed fixture, because the fixture is
what proves the *decoder* can carry such a column name at all.

Fixture location and resolution follow #3504 verbatim, for the reason recorded in
`test-data/schemas/issue-3504-udt-collision.cql`: committed **checkout-relative** under
`test-data/fixtures/issue_3630/`, never under `test-data/datasets/sstables/`, because both binding
suites resolve the corpus from `CQLITE_DATASETS_ROOT` and never fall back to the checkout — a
corpus-rooted fixture is invisible on exactly the runs that must see it. Absence is therefore a
broken checkout and MUST fail closed, never skip (#3220's per-case `must_run` rule).

## D3 — The JSON-object subject needs a CQLite schema, because `json` is not a Cassandra type

`Value::Json` is produced in exactly one place —
`cqlite-core/src/storage/sstable/reader/parsing/custom_scalar.rs`'s `"json"` arm, reached when the
**schema** declares a column of CQLite's `json` type, which becomes
`ComparatorType::Custom("json")`. `json` is **not** a Cassandra CQL type, so **no Cassandra-written
schema can produce this cell**: the subject is necessarily a Cassandra-written **`text`** column
holding a JSON document, read through a **committed CQLite schema that declares that column as
`json`**. That is schema-driven, not a heuristic (the `Custom(name)` string comes from the schema —
the module's own header says so), so it is doctrine-clean.

This is why the issue records the JSON half as needing a fixture (`docs/development/M4_spec.md`
row **b-5**). The implementer MUST verify this path end to end before writing the assertion, and if
the declared-`json`-over-`text` route does not in fact reach `json_to_napi`, the JSON half is
reported as **unreachable from the public surface** with the measurement that shows it — and the
`Object.create(null)` change to `json_to_napi` then ships with Rust-level coverage plus an explicit
"no public-surface wiring evidence" note, rather than a fabricated green. Do **not** substitute a
CQLite-written round-trip and call it an oracle: a CQLite-written + CQLite-read test is invariant to
a uniform error on both sides (CLAUDE.md, #3042).

**Nested objects take the same contract.** `json_to_napi` recurses, so an inner object must be
null-prototype too — the requirement is over the construction, not over the top level.

## D4 — What must not move

- **`Object.keys(row)` order.** #1446's contract: equal to `columns.map(c => c.name)` in
  authoritative SELECT order, extras name-sorted after. `napi_define_properties` defines in array
  order and V8 preserves string-key insertion order for own properties, so M1/M2 preserve it — but it
  is asserted, not assumed, because the whole point of the #3504 precedent is that a `toHaveLength(3)`
  assertion could not see a lost key.
- **The metadata-column-with-no-value skip.** A metadata column absent from `values` is skipped, not
  null-filled (aggregate queries key the value by the expression name while metadata carries
  `col_0`). Under a descriptor-array mechanism the temptation is to build one descriptor per declared
  column; it must stay one per *present* column.
- **`udt_to_object`.** #3504's null-prototype field bag, and the outer object's normal prototype,
  stay exactly as they are.
- **The error contract.** Any new refusal goes through `to_napi_error` / the one FFI error contract,
  as `json_number_to_napi`'s `Beyond` arm does.

## D5 — Where this is covered

- `bindings/node/__test__/issue-3630-row-key-namespace.test.js` — the public-surface wiring evidence
  (jest, run in the gate's `node-bindings` component, which since #3522 runs the WHOLE suite).
- `bindings/node/src/value_tests.rs` — Rust-level unit coverage, executed by the gate's
  `binding-rust-tests` component (#3522).
- `bindings/python/tests/` — the AC8 assertion that the Python row path has no analogous hole.
- `test-data/scripts/check-dataset-manifest.sh` is **not** the right home for the fixture check: this
  fixture is checkout-relative and not part of the fetched corpus. The test fails closed on its
  absence itself.
