# Design: Delta-Scan Envelope for CDC-Style Parquet Projections

**Status:** Validated design (brainstormed 2026-06-10, pending review)
**Audience:** CQLite maintainers
**Related:**
- `docs/architecture/cassandra-sidecar-parquet-projections.md` (decisions this design implements)
- `docs/architecture/delta-scan-consumer-reconciliation.md` (consumer merge recipe — DuckDB SQL reference)
- Epic #673 (Arrow type fidelity), Epic #682 (Parquet writer in core), Epic #689 (WRITETIME/TTL in SELECT)
- Issue #493 (set element tombstones), Issue #667 (tombstone readback coverage)

## Purpose

A flushed SSTable is a delta, not a snapshot. Projecting it to Parquet correctly
requires carrying cell write-timestamps and representing every delete shape
Cassandra produces — otherwise a downstream union of per-flush files resurrects
deleted data and merges stale cells. This document defines the **CQLite-native
envelope** (per the recorded decision: not Debezium-compatible) and the core API
that produces it.

**Contract in one sentence:** one SSTable generation in, faithful change events
out; reconciliation (LWW merge, tombstone application, TTL filtering) is
deliberately the downstream consumer's job.

## Decisions

| Question | Decision |
|---|---|
| Record grain | Row-grain records; each non-key column is a `Struct{value, writetime, expires_at}` |
| Per-cell vs per-row writetime | Per-cell, via the struct; record-level `__ts` carries row liveness / deletion time |
| Absent vs null | Null struct = "cell not in this delta"; `{value: null, writetime: t}` = cell tombstone |
| Non-cell deletes | Single stream with `__op` discriminator; typed range-bound structs |
| TTL | `expires_at` (µs epoch) on the cell struct; never resolved at scan time (idempotent output) |
| Static columns | Dedicated `static_upsert` record per partition (clustering keys null) |
| API surface | New feature-gated core scan API on the reader layer; not a SELECT extension; not CLI-only |

## Envelope schema

Each table's delta schema is derived from its CQL schema plus fixed envelope
columns. All Arrow types follow the epic #673 fidelity mapping.

### Columns

1. **Key columns** — partition key and clustering columns as plain Arrow types.
   Populated per record type:

   | `__op` | partition key | clustering |
   |---|---|---|
   | `upsert` | yes | yes |
   | `static_upsert` | yes | null |
   | `row_delete` | yes | yes |
   | `range_delete` | yes | null (bounds carry clustering) |
   | `partition_delete` | yes | null |

2. **Cell columns** — every non-key column becomes:

   ```
   Struct {
     value:      <Arrow type per #673>,  // null = cell tombstone
     writetime:  i64,                    // µs since epoch, required
     expires_at: i64 | null,             // µs since epoch; null = no TTL
   }
   ```

   The struct itself is nullable: a null struct means the cell is not present
   in this generation (e.g. a partial UPDATE touched other columns).

3. **`__op: Utf8`** (dictionary-encoded) — `upsert | static_upsert |
   row_delete | range_delete | partition_delete`.

4. **`__ts: i64 | null`** — for upserts, the row's primary-key liveness
   timestamp (null when the row was created by UPDATE and has no liveness
   info); for all delete ops, the deletion timestamp (`markedForDeleteAt`).

5. **`__range_start` / `__range_end`** —
   `Struct{<clustering columns>, inclusive: bool}`, null except on
   `range_delete`. Bounds are typed in the table's own clustering types and
   may be prefixes: trailing clustering components are null.

### Examples

Table `t (pk int, ck text, val text, st text STATIC, PRIMARY KEY (pk, ck))`:

```
-- UPDATE t SET val='x' WHERE pk=1 AND ck='a'   (partial update)
{ pk:1, ck:'a', __op:'upsert', __ts:null,
  val:{value:'x', writetime:t1, expires_at:null}, st:null }

-- DELETE val FROM t WHERE pk=1 AND ck='a'      (cell tombstone)
{ pk:1, ck:'a', __op:'upsert', __ts:null,
  val:{value:null, writetime:t2, expires_at:null}, st:null }

-- UPDATE t SET st='S' WHERE pk=1               (static write)
{ pk:1, ck:null, __op:'static_upsert',
  st:{value:'S', writetime:t3, expires_at:null}, val:null }

-- DELETE FROM t WHERE pk=1 AND ck='a'          (row tombstone)
{ pk:1, ck:'a', __op:'row_delete', __ts:t4, val:null, st:null }

-- DELETE FROM t WHERE pk=1 AND ck >= 'a' AND ck < 'm'
{ pk:1, __op:'range_delete', __ts:t5,
  __range_start:{ck:'a', inclusive:true},
  __range_end:{ck:'m', inclusive:false} }

-- DELETE FROM t WHERE pk=1                     (partition tombstone)
{ pk:1, __op:'partition_delete', __ts:t6 }
```

### Downstream merge keys

Consumers reconcile with: `upsert`/`row_delete` on `(pk, ck)`;
`static_upsert` on `pk`; `partition_delete` and `range_delete` applied as
predicates over `(pk)` / `(pk, ck range)` with `__ts` deciding
last-write-wins per cell.

### Name collisions

A user column named `__op` (legal in CQL) collides with the envelope. The
writer **errors at schema-derivation time** — no silent renaming — with an
`--envelope-prefix` option to choose a different reserved prefix.

### Non-frozen collections (v1 limitation)

Multi-cell collections carry per-element writetimes and element tombstones
(issue #493 territory). V1 emits the collection cell as:

- `value`: the elements present in this generation (for an append
  `s = s + {...}`, that is only the appended elements — correct delta
  semantics)
- `writetime`: max element writetime
- `replaced: bool` added to the struct for collection columns only — `true`
  when the generation carries a collection tombstone (i.e. `s = {...}`
  overwrite), so consumers know whether to merge or replace

Element-level removals are detected and counted (scan summary warning) but not
represented in v1; full element fidelity is a tracked follow-up.

## API and architecture

### Core API

Feature `delta-scan`, off by default. Lives on the SSTable reader layer
(`row_cell_state_machine`, the V5 parsers, `TombstoneInfo`/`TombstoneType`
from `types.rs`) — **not** the query engine, whose contract (merge
generations, suppress tombstones) is the opposite of this one.

```rust
pub enum DeltaRecord {
    Upsert      { keys: RowKeys, liveness: Option<CellMeta>, cells: Vec<(ColumnId, CellDelta)> },
    StaticUpsert{ partition_key: RowKeys, cells: Vec<(ColumnId, CellDelta)> },
    RowDelete   { keys: RowKeys, deleted_at: i64 },
    RangeDelete { partition_key: RowKeys, start: RangeBound, end: RangeBound, deleted_at: i64 },
    PartitionDelete { partition_key: RowKeys, deleted_at: i64 },
}

pub struct CellDelta {
    pub value: Option<Value>,      // None = cell tombstone
    pub writetime: i64,
    pub expires_at: Option<i64>,
    pub replaced: bool,            // collections only; see v1 limitation
}

pub fn scan_delta(sstable_dir: &Path, table: &TableSchema)
    -> impl Stream<Item = Result<DeltaRecord>>
```

Records stream in SSTable order (partition, then clustering). No
cross-SSTable merge, no GC-grace filtering.

### Envelope writer

`DeltaParquetWriter` in core behind `delta-scan` + `parquet` (composes with
the epic #682 lifted writer). Derives the schema above from `TableSchema`,
consumes the record stream, writes one file per generation.

Parquet footer key-value metadata (enough for an external committer to dedupe
and order generations):

| Key | Value |
|---|---|
| `cqlite.delta.version` | `1` |
| `cqlite.delta.source` | SSTable identity/generation |
| `cqlite.delta.schema_hash` | hash of the CQL schema used |
| `cqlite.version` | crate version |

### CLI

```bash
cqlite delta-export <sstable-dir> --schema <file.cql> --out parquet -o <file>
```

Bindings exposure is deferred until there is a consumer.

## Error handling

1. **No silent skips.** Any structure the scanner cannot faithfully represent
   is a hard error naming the partition/position — never a dropped record
   (no-heuristics mandate, issue #28).
2. **Fail before writing.** Collision and unsupported-feature errors (e.g.
   counter tables) are raised at schema-derivation time, before any output
   bytes exist.
3. **Loud limitations.** Collection element tombstones are detected and
   surfaced as a warning counter in the scan summary, not merged away
   silently.

## Testing

1. **Unit** — synthetic cells/tombstones through the record builder: every
   `__op` shape, null-vs-absent struct semantics, TTL, liveness-less upserts,
   prefix range bounds, collision errors.
2. **Fixture parity** — for each corpus table, `scan_delta` output must agree
   with `sstabledump` JSONL (which natively shows tombstones, `tstamp`, and
   expiration — effectively a reference delta scan). Requires fixtures with
   deletes; coordinate with #667 (tombstone readback coverage) rather than
   duplicating fixture work.
3. **Reconciliation round-trip** (the proof of sufficiency) — write N flush
   generations from a real Cassandra container, delta-export each, run the
   documented consumer merge in DuckDB/Spark SQL, and assert the result equals
   CQLite's own merged `SELECT *` over all generations.

## Sequencing

- **Depends on:** epic #673 (type fidelity — the cell struct `value` field
  reuses its mapping) and epic #682 (Parquet writer in core).
- **Independent of:** epic #689 (WRITETIME/TTL in SELECT) — different layer,
  shared fixture interests only.
- **Next step:** after this design is reviewed, scaffold the implementation
  epic (scanner, schema derivation, writer, CLI, three test tiers).

## Explicitly out of scope

- Cross-SSTable merge or snapshot semantics (use the existing read path)
- GC-grace handling, compaction awareness, commitlog/CDC reading
- Iceberg/Delta commit logic (external committer's job, per recorded decision)
- Collection element tombstone fidelity (v1 limitation above)
- Counter tables (rejected at schema derivation)
