# Tasks: Trino typed collection columns

## 1. ArrowTypeMapper — recursive complex-type mapping
- [ ] 1.1 Add `ArrowType.List` → `ArrayType(element)`, `ArrowType.Struct` → `RowType(named fields)`,
      `ArrowType.Map` → `MapType(key, value)` arms to `toTrinoOrEmpty`, recursing on child `Field`s and
      reusing the existing leaf mapping. (surface: `ArrowTypeMapper.toTrinoOrEmpty`)
- [ ] 1.2 A collection/struct is `Optional.empty()` iff a leaf recurses to empty; complex columns get
      `PushdownCapability.NONE`.
- [ ] 1.3 Unit tests: List(Utf8)→array(varchar); Struct{street,zip}→row(...); Map(Utf8,Int)→map(...);
      nested List(List(Utf8)); unmappable-leaf collection → empty. (exercises `ArrowTypeMapper`)

## 2. ArrowToTrino — materialize complex vectors
- [ ] 2.1 Add `ListVector`/`StructVector`/`MapVector` handling to build ARRAY/ROW/MAP blocks, recursing to
      the existing scalar writers per element/field/entry. (surface: `ArrowToTrino.writeValue`/`toBlock`)
- [ ] 2.2 Null collection cell → `appendNull`; empty collection → empty non-null block. Map entries read
      from the Arrow entry `Struct(key,value)`.
- [ ] 2.3 Extend the `ArrowTypeMapperTest` round-trip so every accepted Arrow type also materializes
      through `ArrowToTrino` (lockstep invariant, #2679-class guard).

## 3. Loud + durable hide (independent "at minimum" patch)
- [ ] 3.1 `CqliteFlightMetadata.supportedFields`: emit the hidden-column WARNING (column name + Arrow
      type) on every projection that hides columns, not once per table per instance.
- [ ] 3.2 Test: a still-unsupported column is warned on two successive metadata loads; a
      fully-unsupported table still fails loudly with `NOT_SUPPORTED`.

## 4. End-to-end wiring evidence (docker-compose Trino)
- [ ] 4.1 E2E test: create `cassandra_easy_stress.udt_simple(key text PRIMARY KEY, addrs
      list<frozen<simpleaddr>>)`, insert a multi-element row, an empty-list row, and an absent-`addrs` row.
- [ ] 4.2 Assert through Trino: `DESCRIBE` shows `addrs array(varchar)`; `SELECT addrs` resolves and
      returns the elements; `SELECT *` includes `addrs`; empty→empty array, absent→null.
- [ ] 4.3 Assert element-for-element the array VARCHARs equal the server-decoded UDT strings (oracle vs
      the on-disk data / sstabledump rendering).

## 5. Docs (same change)
- [ ] 5.1 Update the connector supported-types note (`flight-trino-user-docs` / connector README):
      collections are now projected; element/field/key/value limited to the connector's scalar leaf set;
      UDT elements render as VARCHAR pending #2349.

## 6. Quality gates
- [ ] 6.1 Java connector build + tests green (gradle); `scripts/agent-gate.sh` PASS (the one gate of
      record, inside flow-closer).
- [ ] 6.2 C intent audit (spec-auditor) PASS — every requirement `satisfied` with a public-surface test.
- [ ] 6.3 roborev clean (blockers fixed; nits batched to a follow-up issue).
