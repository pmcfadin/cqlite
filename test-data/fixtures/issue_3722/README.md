# issue_3722 — Cassandra-written UDT FIELD-TYPE coverage fixture

Backs **issue #3722**: UDT *field* values of many CQL types decode as an opaque
`Value::Blob`. Two separate shared UDT-field decoders in
`cqlite-core/src/storage/sstable/reader/parsing/row_decoder/udt.rs` —
`parse_udt_field_value` and `parse_simple_udt_field_value` — have **divergent arm
sets** and **both end in `_ => Value::Blob`**, so a field type neither names
silently becomes bytes.

Before this fixture the defect was **unreachable from the corpus**: a census of
every `CREATE TYPE` in `test-data/schemas/**` (7 files) found not one declaring
`smallint`/`tinyint`/`decimal`/`varint`/`time`/`timeuuid`/`duration`, and only
`unhashable_fields` declared a collection field.

- Keyspace / table: **`test_udt_wide_fields.udt_wide_fields`**
- Fixture root (itself an *sstables root* — it directly contains the keyspace
  directory): `test-data/fixtures/issue_3722`
- Generation: `test-data/scripts/generate-issue-3722-udt-wide-fields.sh`
- Schema: `test-data/schemas/issue-3722-udt-wide-fields.cql` (read it for the
  per-field and per-column rationale)
- sstabledump JSONL golden:
  `test_udt_wide_fields/udt_wide_fields-60359590a5c911f1b73505487f32dd64/nb-1-big-Data.db.jsonl`
  (3 partitions), alongside `nb-1-big-Statistics.db.txt` (`sstablemetadata`)

**Cassandra-written, not CQLite-written.** Mandated by CLAUDE.md: for an on-disk
decode property the oracle must be Cassandra-written bytes. A
CQLite-write/CQLite-read round trip is *invariant* to a uniform decode error and
cannot detect this defect.

**Checkout-relative on purpose.** Consumers resolve the corpus from
`CQLITE_DATASETS_ROOT`, which every gate run sets, so a corpus-rooted fixture is
invisible on every gate run. A checkout-relative path cannot be hidden by an env
var. Precedent: `test-data/fixtures/issue_3504/`.

## Two deliberate controls

| field | role |
|---|---|
| `bl blob` | the **one** field that MUST still decode to `Value::Blob` — catches a fix that blanket-stops emitting Blob |
| `i int`   | already decodes correctly today — catches a regression in the working path |

Every other scalar value is self-describing and sign-bearing so a wrong decode is
*visible* rather than merely absent: `s: -300` = `0xfed4` (a wrong-width read
cannot produce -300), `t: -1` = `0xff`, `d: 123.45` = scale 2 / unscaled 12345,
`vi` exceeds `i64`, `du` has all three components non-zero. **Preserve that
property in any row added later.**

## MEASURED REFUSAL — do not retry

`counter` is refused as a UDT field by Cassandra 5.0.2, verbatim:

```
InvalidRequest: code=2200 [Invalid query] message="A user type cannot contain counters"
```

Issue #3722's AC1 names `counter`, so that AC is **unsatisfiable by
construction** and no fixture can carry it; the `counter` arm is pinned at the
`CqlType` level instead.

## Authoritative marshal spelling (AC2)

The `wide` UDT as it appears in this SSTable's `SerializationHeader`
(`org.apache.cassandra.db.marshal.` elided as `M.` for width; the real string
carries the full package on **every** type, and is 1207 characters):

```
M.UserType(test_udt_wide_fields,77696465,
  73:M.ShortType, 74:M.ByteType, 64:M.DecimalType, 7669:M.IntegerType,
  746d:M.TimeType, 7475:M.TimeUUIDType, 6475:M.DurationType,
  6474:M.SimpleDateType, 6970:M.InetAddressType,
  666c:M.ListType(M.Int32Type), 6673:M.SetType(M.UTF8Type),
  666d:M.MapType(M.UTF8Type,M.Int32Type),
  7470:M.TupleType(M.Int32Type,M.UTF8Type),
  6e75:M.UserType(test_udt_wide_fields,696e6e65725f75,61:M.Int32Type,62:M.UTF8Type),
  626c:M.BytesType, 69:M.Int32Type)
```

Field names are **hex-encoded** (`77696465` = `wide`, `696e6e65725f75` =
`inner_u`, `73` = `s`, `7669` = `vi`, …) and appear in **declaration order**,
which is what the header records — do not reorder the `CREATE TYPE` without
regenerating the fixture.

### MEASURED: the same UDT has TWO spellings in this one fixture

Per-column, with the `wide` UserType elided as `<WIDE>`:

| column | declared CQL | marshal spelling |
|---|---|---|
| `w`   | `frozen<wide>`                   | `FrozenType(<WIDE>)` |
| `mw`  | `map<frozen<wide>, int>`         | `MapType(FrozenType(<WIDE>),Int32Type)` |
| `fmw` | `frozen<map<frozen<wide>, int>>` | `FrozenType(MapType(<WIDE>,Int32Type))` |
| `fsw` | `frozen<set<frozen<wide>>>`      | `FrozenType(SetType(<WIDE>))` |
| `sw`  | `set<frozen<wide>>`              | `SetType(FrozenType(<WIDE>))` |

Inside an **already-frozen** container (`fmw`, `fsw`) Cassandra emits the element
/ key UDT with **no `FrozenType(...)` wrapper**; at top level and inside a
**multicell** container (`w`, `mw`, `sw`) it does. A decoder keyed on
`FrozenType(UserType(...))` therefore reaches three of the five columns and
misses two. This was measured from the generated fixture, not predicted.

## MEASURED: multicell containers deliver the UDT as a CELL PATH

`mw` and `sw` are multicell, so the `wide` value arrives as a cell **path**, not
a value — and `sstabledump` renders that path as a single colon-joined **string**
in which the UDT's own collection fields appear **hex-encoded**, e.g.

```
-300:-1:123.45:90071992547409910000:13\:30\:54.234000000:…:00000003000000040000000100000004fffffffe0000000400000003:…
```

`fmw` and `fsw` are frozen: one value cell each, rendered structurally. So the
frozen and multicell halves of #3722's AC3 are genuinely different code paths and
both halves of each near-sibling pair stay.

## Rows

| id | what it pins |
|---|---|
| 1 | every column populated with the full 16-field value — all five AC3 container routes |
| 2 | **NULL UDT fields**: `w` with 6 fields null (3 scalars `t`/`vi`/`du`, 2 collections `fs`/`fm`, the nested UDT `nu`) and the rest populated, incl. both controls — proves the absent-field encoding is orthogonal to field-type decoding |
| 3 | a **second, distinct** `wide` value in `mw`/`fmw`/`fsw`/`sw` so multi-element ordering/uniqueness are exercised by Cassandra's own comparator; inserted **before** id 2 and with the second value written **first** in every literal. MEASURED: Cassandra sorts `s: -300` before `s: 32767` in all four containers, i.e. the committed bytes reflect Cassandra's ordering, not the insertion order |

`w` is NULL in row 3 so a failure decoding the top-level UDT cannot mask the
container routes; rows are split wherever a decode failure in one column could
mask another.

## Determinism

Uncompressed (no `CompressionInfo.db`, asserted by the generator),
`SizeTieredCompactionStrategy`, exactly one `nodetool flush` ⇒ exactly one
`Data.db` (asserted). Every INSERT carries `USING TIMESTAMP 1000` so the golden
is reproducible instead of carrying a wall clock. **Not fully deterministic**: a
multicell collection INSERT (`mw`, `sw`) also emits a collection tombstone whose
`local_delete_time` comes from `nowInSeconds`, which no CQL clause pins.

The `*.db` binaries are gitignored and were committed with `git add -f`, per
CLAUDE.md's "Gitignored reference binaries" doctrine.
