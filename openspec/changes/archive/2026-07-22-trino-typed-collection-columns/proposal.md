# Proposal: Project Cassandra collection columns through the Trino connector

## Milestone / theme
0.15 — cqlite-trino latency/throughput/operations theme (epic #2403). Fixes issue #2815.

## Routing
**Design-driven** (Trino connector capability gap — type-mapping + Arrow materialization surface).
Not oracle-driven: no SSTable parse/decode change; the server-side Arrow schema is already correct.

## Problem

On 0.16.0-rc2, `list<frozen<udt>>` (and every other collection) column is **silently dropped** from the
projected Trino schema: it is absent from `DESCRIBE`, `SELECT *` omits it, and `SELECT <col>` fails to
resolve — with no error and no durable log. A caller cannot tell the column exists. This is **silent data
loss** for any collection column via Trino.

Confirmed in source, two layers:

1. **The drop (primary).** `ArrowTypeMapper.toTrinoOrEmpty` is an exhaustive switch over *flat* Arrow
   types only (Bool/Int/Float/Utf8/Binary/Date/Time/Timestamp); Arrow `List`, `Struct`, and `Map` all
   fall to `default -> null` → `Optional.empty()` → `CqliteFlightMetadata.supportedFields` **hides the
   column**. The server side is already correct: `cqlite-core/src/export/arrow_convert.rs` emits
   `list<frozen<udt>>` as Arrow `List(...)` in `GetSchema`. So this is a **pre-existing connector
   capability gap** — *no collection column of any element type has ever been projectable through Trino*.
   It surfaced now because #2807 unblocked UDT tables far enough for the schema to come back at all.

2. **The "no log."** `supportedFields` already logs a WARNING naming each hidden column + its Arrow type,
   but guards it with `warnedHiddenColumns.add(tableName)` — **once per table per connector instance**, so
   it fires on the first metadata load and is invisible on every later `DESCRIBE`. Easy to miss entirely.

## What we will build (connector-side only)

Teach the Trino connector to project Cassandra collection columns as typed Trino complex columns:

- `ArrowTypeMapper`: recursively map Arrow `List` → Trino `array(element)`, `Struct` → `row(named
  fields)`, `Map` → `map(key, value)`, reusing the existing flat-leaf mapping for element/field/key/value
  types and rejecting only a genuinely unmappable *leaf*.
- `ArrowToTrino`: materialize Arrow `ListVector` / `StructVector` / `MapVector` into Trino ARRAY / ROW /
  MAP blocks — recursively, delegating each leaf to the existing scalar writers. **Mapper and materializer
  land together, gated by an end-to-end `SELECT` of a `list<frozen<udt>>` through docker-compose Trino**,
  so the connector never advertises a type the reader cannot build (the #2679-class trap).
- Hardening: make the hide **loud and durable** — emit the hidden-column WARNING every time a table's
  columns are hidden (drop the once-per-table suppression, or make it per-hidden-set), so any column that
  still cannot be mapped is discoverable, never silently omitted.

With this, `list<frozen<udt>>` projects as `array(varchar)` whose elements are the server-decoded UDT
strings (the same rendering scalar `frozen<udt>` already gets as VARCHAR). Silent data loss is gone, the
column is queryable, and the string elements remain a valid correctness oracle vs `sstabledump` — which
also un-blocks field verification of #2349's in-collection decode via the now-visible array column.

## Non-goals

- **Typed UDT elements** (`array(row(street varchar, zip integer))`) — requires resolving the server-side
  `Custom("udt:ks.name")` type through the UDT registry into a real Arrow `Struct` before
  `build_arrow_schema`. That server-side registry wiring is #2349's scope; it is explicitly deferred here.
  Elements stay `varchar` (server-decoded UDT strings) in this change.
- **Scalar `frozen<udt>` behavior is unchanged** — it already projects as VARCHAR and stays VARCHAR.
- **Composite `SET<FROZEN<udt>>` / map-key UDT elements (#2339)** — unchanged known gap.
- **Predicate pushdown into collection columns** — collections keep `PushdownCapability.NONE` (Trino
  post-filters); no pushdown for complex columns in this change.
- **No CQL parsing or SSTable decode changes** — the Rust core is untouched except tests, if any.

## Doctrine impact

- Wiring-evidence: the feature is done only when a real `SELECT` of a collection column through Trino
  returns the expected rows end to end — a mapper unit test alone is insufficient.
- Update the connector user docs (`flight-trino-user-docs`) supported-types note: collections are now
  projected (element/field/key/value types limited to the connector's scalar leaf set; UDT elements render
  as VARCHAR pending #2349).
