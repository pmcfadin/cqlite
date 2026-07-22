# Design: Trino typed collection columns

## Context

`cqlite-flight`'s `GetSchema` already emits Cassandra collection columns as Arrow `List`/`Struct`/`Map`
(verified: `cqlite-core/src/export/arrow_convert.rs`, `cql_type_to_arrow_field`). The gap is entirely in
the Java Trino connector:

- `ArrowTypeMapper.toTrinoOrEmpty` handles flat Arrow types only; `List`/`Struct`/`Map` → `default -> null`.
- `CqliteFlightMetadata.supportedFields` drops any column the mapper returns empty for, and its
  hidden-column WARNING is guarded once-per-table (`warnedHiddenColumns`).
- `ArrowToTrino.writeValue` has no `ListVector`/`StructVector`/`MapVector` cases.

## Chosen approach: recursive connector-side mapping + materialization, elements stay VARCHAR

Extend the two connector classes symmetrically and land them together behind a docker-compose E2E gate:

1. **`ArrowTypeMapper`** — add `ArrowType.List`, `ArrowType.Struct`, `ArrowType.Map` arms that recurse on
   child `Field`s and build `ArrayType` / `RowType` (named fields) / `MapType` from the already-supported
   leaf mapping. A collection is unsupported iff a *leaf* recurses to empty. Keep the UUID-extension and
   pushdown-capability handling as-is; complex columns get `PushdownCapability.NONE`.

2. **`ArrowToTrino`** — add `ListVector`/`StructVector`/`MapVector` (and `MapVector`'s entry `StructVector`)
   handling to build ARRAY/ROW/MAP blocks, recursing to the existing scalar writers per element/field/
   entry. Preserve null-cell → `appendNull`, empty-collection → empty non-null block. The existing
   `ArrowTypeMapperTest` round-trip (every accepted type must materialize) is extended to cover the new
   types, enforcing the lockstep invariant in a unit test.

3. **Loud + durable hide** — change `supportedFields` so the hidden-column WARNING is emitted on every
   projection that hides columns (remove the `warnedHiddenColumns` gate, or key it so it is not
   permanently suppressed). This is the small, independent "at minimum" patch from the issue and lands in
   the same change.

4. **E2E wiring evidence** — a docker-compose Trino test creates a `list<frozen<udt>>` table, inserts
   rows (multi-element, empty, absent), and asserts `DESCRIBE`, `SELECT *`, and `SELECT addrs` return the
   `array(varchar)` column with server-decoded UDT-string elements. This is the wiring-evidence gate; the
   mapper unit test alone is insufficient.

### What it beat

- **Full typed rows (`array(row(...))`)** — rejected for this change (owner decision): it requires
  server-side `Custom("udt:ks.name")` → Arrow `Struct` resolution through the UDT registry, entangled with
  #2349, spanning the Rust core. Deferred to #2349; elements stay VARCHAR here. This keeps the change
  connector-only, unblocks the P1 data-loss immediately, and preserves the sstabledump string oracle.
- **Hide-only louder patch (no typed columns)** — rejected as insufficient: it makes the drop
  discoverable but still returns no data for the column; the issue asks for the column to be queryable.
- **Server-side collection→string flattening** — rejected: the server already emits correct Arrow
  `List`; re-flattening server-side would discard structure the connector can now represent as `array`.

## Risks / edge cases

- **Trino ROW field-name/order fidelity** — build `RowType` from the Arrow struct children preserving
  name + order; a positional-only ROW would misalign fields. Covered by the Struct scenario.
- **Map entry representation** — Arrow `MapVector` stores entries as a `Struct(key,value)` list; the
  materializer must read the entry struct, not assume parallel key/value vectors.
- **Null vs empty collection** — must stay distinguishable (null cell vs empty block); covered by a
  scenario.
- **Lockstep drift** — the mapper must never advertise a type the materializer can't build; the extended
  round-trip test is the guard (#2679-class trap).
- **Nested unmappable leaf** — a collection whose leaf is unsupported must make the *whole column*
  unsupported (and now loudly hidden), not partially materialize.
