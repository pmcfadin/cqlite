# Proposal: a column name is DATA and SHALL NOT reach JavaScript's object model

**Issue**: #3630. **Milestone**: maintenance (bindings correctness), 0.15 line.
**Routing**: **design-driven**. There is no oracle. The Cassandra format and `sstabledump` say
nothing about how a language binding should surface a row; the choice of *which* JavaScript object
shape a row is has public-surface consequences with real latitude, and the issue's own AC1 is
*"an OpenSpec change records the chosen row contract and the (possibly different) chosen
JSON-object contract"*. Hence a proposal rather than a pinned parity test. (Contrast: the *decode*
of a row is oracle-driven and is untouched here.)

## Problem

Three sites in `bindings/node/src/value.rs` write a **user-controlled property name** onto an object
that inherits from `Object.prototype`, using an ordinary JavaScript `[[Set]]`
(`napi_set_property` / `napi_set_named_property` are both `[[Set]]`). `Object.prototype` has a
`__proto__` accessor, so a name equal to `__proto__` reaches that **inherited setter** instead of
creating an own property:

| # | Site | Where the name comes from |
|---|---|---|
| 1 | `row_to_object`, interned-key path (`value.rs:590`) | authoritative SELECT column names — user-controlled via a quoted CQL identifier or a `SELECT … AS` alias |
| 2 | `row_to_object`, extras path (`value.rs:610`) | value-map keys the column list does not cover |
| 3 | `json_to_napi`, object-key loop (`value.rs:396-398`) | the JSON document, i.e. the data itself |

Measured on the Cassandra-5.0.2-written `test-data/fixtures/issue_3504` fixture for the UDT field
bag before #3504 fixed it, by the identical mechanism:

- a **string-valued** `__proto__` **vanishes** — absent from `Object.keys`, not an own property,
  `obj.__proto__` reading back `Object.prototype`. The column is **silently lost**, no error
  anywhere.
- a **null-valued** one instead **replaces the object's prototype** with `null` (assigning `null` or
  an object to `__proto__` is the one case the accessor honours), so the row silently becomes a
  null-prototype object.

Blast radius is larger than #3504's: **every row of every result set** goes through
`row_to_object`, where the UDT bag was reached only by rows containing a UDT cell.

This is the control/data channel-sharing shape `CLAUDE.md` records as the umbrella lesson of #3312
— a control channel placed in a namespace the data controls — now one layer below CQLite, in the
host language's own object model. #3504 closed it for the UDT field bag with
`Object.create(null)` and **deliberately scoped the row and JSON paths out**, because a row is a
documented plain-object surface and the remedy there is a design call. This is that call.

## Decision (AC1) — two contracts, deliberately different

**Rows keep `Object.prototype`, and every column becomes an own enumerable data property defined
WITHOUT `[[Set]]`.** JSON-object cells become **null-prototype** objects, like the UDT field bag.

The two halves differ because the two surfaces differ, and the difference is exactly whose names
they are:

- A **row**'s key set is a *declared, finite, authoritative* thing (the SELECT column list). Rows are
  a **documented plain-object surface**: `bindings/node/lib/index.d.ts:157-158` types them as
  `export interface Row { [column: string]: Value }`, and consumers call `row.hasOwnProperty(...)`,
  spread them, `JSON.stringify` them, and hand them to code that expects a normal prototype.
  `Object.create(null)` — #3504's remedy for the *field bag* — would break
  `row.hasOwnProperty(...)`, `row.toString()` and `row instanceof Object` on **every** row of
  **every** query, to fix a name almost no schema uses. That is a breaking change to the most-used
  surface in the binding, charged to every consumer.
- A **JSON object cell** is a *data mapping* whose keys are the data, with no declared key set at all
  — much more like the UDT field bag than like a row. There, `obj[k] === undefined` meaning exactly
  "no such key" is worth more than `obj.hasOwnProperty` existing, and the bag is not a documented
  named surface anyone probes with `instanceof`.

**Both halves remove the shared channel rather than narrowing it.** Neither is a special case on the
literal string `__proto__` — a doctrine constraint carried over from #3504 verbatim, recorded at
`bindings/node/src/value.rs:492-495`. A literal-name check is *picking a rarer delimiter*: it leaves
every other inherited name, including any a future JavaScript adds to `Object.prototype`, able to
intercept a declared column. For rows the channel is removed by never performing a `[[Set]]`; for
JSON objects by there being no prototype to consult.

**Rejected alternatives, in the order they fail:**

- **`Object.create(null)` for rows too** (issue remedy 2). Removes the channel, and is the cheapest
  code change — but it breaks the documented contract above on every row. Rejected on blast radius,
  not on principle.
- **A `Map`-valued or dual-shape row API** (issue remedy 3). The only option that removes the
  name/namespace collision *by construction* rather than by property-definition semantics, and the
  strongest long-term answer — but it is a wholesale redesign of the binding's primary return type,
  breaks every existing consumer, and is not this issue. Recorded as the direction a v1.0 API freeze
  should weigh (follow-up, not scope).
- **A literal `__proto__` special case.** Forbidden by the issue and by doctrine. Named here only so
  the record shows it was considered and refused.
- **Rejecting a column named `__proto__` at read time** (the shape #3504 rejected as its option (c)).
  It refuses data Cassandra accepts and the CLI already reads correctly — converting a rendering
  defect into a permanent capability hole.

## Accepted cost, stated

Own-property definition keeps `Object.prototype`, so `'toString' in row` stays true and
`row.constructor` stays truthy: an **absence probe by `in` or by truthiness still reads inherited
junk**. This is exactly the tradeoff `udt_to_object`'s doc comment weighs at `value.rs:496-500`, and
it is accepted here and *not* there, because a row has an authoritative column list to probe
(`result.columns`) and a field bag does not. `Object.hasOwn(row, name)` is the correct absence probe
on a row and is documented as such.

**One residual is outside the binding's reach and is recorded rather than fixed:**
`Object.assign(target, row)` performs `[[Set]]` on *`target`*, so a consumer who copies a row into a
fresh `{}` re-loses a `__proto__` column. `{...row}` and `Object.fromEntries(Object.entries(row))`
do not (both define rather than set). Nothing in the binding can prevent a caller's own `[[Set]]`;
the fix is to stop the binding from being the one that loses data.

## Non-goals

- **Site 2 of #3504's table — the cell-level `map` ambiguity** — remains #3497's. Untouched.
- **A `Map`-valued row API / dual-shape results.** See rejected alternatives; a v1.0 API question.
- **The Python binding's rendering.** In scope only as an **assertion** (AC8): a Python `dict` has no
  inherited accessors and `dict.__setitem__` consults no prototype chain, so the expectation is *no
  defect*. This change asserts that rather than assuming it, and changes no Python behaviour.
- **Reworking `udt_to_object`.** #3504's null-prototype field bag stands; this change must leave it
  byte-identical in behaviour.
- **Column-order semantics.** #1446 owns `Object.keys(row) === columns.map(c => c.name)` and the
  name-sorted extras order. This change must PRESERVE both exactly, and says so as a requirement.

## Impact

- **No-heuristics mandate**: unaffected. No decode path changes; nothing inspects bytes.
- **Public binding surfaces**: Node only. `Row`'s prototype and key order are unchanged; what changes
  is that a column whose name collides with an inherited accessor now arrives. `index.d.ts` gains the
  documented absence-probe guidance, and a JSON-object member on `Value` (which the union does not
  currently have at all, though `Value::Json` is reachable in the reader — the typing gap is part of
  this change). Python: unchanged.
- **Memory budget**: unaffected — per-row object construction only, no new buffering.
- **Performance**: this is the hottest path in the binding (`row_to_object` runs per row per query),
  and #1446's once-per-result interned `JsString` keys exist to keep it off the `O(rows × columns)`
  re-interning path. The mechanism choice is therefore a measured one, not a free one — see
  `design.md`.
