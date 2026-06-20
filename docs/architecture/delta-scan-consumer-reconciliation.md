# Delta-Scan Consumer Reconciliation Guide

**Applies to:** CQLite delta-scan Parquet envelope (Epic #696, DS10)
**Audience:** Downstream consumers reading per-SSTable delta Parquet files
**Status:** Normative — this is the merge recipe executed by the DS11 round-trip test (Issue #707)
**Related:**
- [Design doc](../plans/2026-06-10-delta-scan-envelope-design.md) — envelope schema, architecture, API
- [Cassandra sidecar Parquet projections](cassandra-sidecar-parquet-projections.md) — integration context
- Issue #493 (collection element tombstones), Epic #696 (delta-scan epic)

---

## Overview

Each CQLite delta-scan Parquet file corresponds to one SSTable generation. The
**envelope schema** carries all the information needed to reconstruct the live
table state from N per-generation files — but reconciliation is explicitly the
downstream consumer's job. CQLite produces files; it does not merge them.

This document specifies every `__op` shape's merge semantics, the null-struct
encoding rules, TTL filtering, collection `replaced` semantics, and a complete
DuckDB SQL reference merge that a consumer can copy-paste and run against real
delta files.

---

## Envelope schema recap

The fields emitted for every record are defined in
`cqlite-core/src/export/delta_schema.rs` and `delta_parquet.rs`. For a table
`t (pk INT, ck TEXT, val TEXT, st TEXT STATIC, PRIMARY KEY (pk, ck))` the
Arrow/Parquet schema is:

```
pk           : Int32                     -- partition key; non-nullable
ck           : Utf8 (nullable)           -- clustering key; null for partition-scoped ops
val          : Struct (nullable) {       -- regular column cell struct
                 value:      Utf8 (nullable),  -- null = cell tombstone
                 writetime:  Int64,             -- µs since epoch, always present
                 expires_at: Int64 (nullable),  -- null = no TTL
               }
st           : Struct (nullable) {       -- static column (same struct shape)
                 value:      Utf8 (nullable),
                 writetime:  Int64,
                 expires_at: Int64 (nullable),
               }
__op         : Dictionary(Int8, Utf8)   -- operation discriminator; non-nullable
__ts         : Int64 (nullable)         -- deletion/liveness timestamp
__range_start: Struct (nullable) {      -- range-delete lower bound
                 ck:         Utf8 (nullable),
                 inclusive:  Boolean,
               }
__range_end  : Struct (nullable) {      -- range-delete upper bound
                 ck:         Utf8 (nullable),
                 inclusive:  Boolean,
               }
```

Key properties verified by `delta_schema.rs` tests:

- `__op` is dictionary-encoded `Int8` → `Utf8`; values are exactly one of the
  five strings below.
- `__ts` is nullable `Int64` microseconds since epoch.
- `__range_start`/`__range_end` are nullable structs, non-null only on
  `range_delete` records. Each clustering column appears in definition order
  within the struct; trailing columns are individually nullable for prefix
  bounds. The `inclusive` field is always non-nullable boolean.
- Cell struct is nullable at the top level (null struct = absent column). Inside
  the struct, `value` is nullable (null value = cell tombstone), `writetime` is
  non-nullable, `expires_at` is nullable.
- Non-frozen collection columns (`list`, `set`, `map`) have a fourth field
  `replaced: Boolean` (non-nullable) in their cell struct. Frozen collections
  and scalars do not.

For multi-column partition or clustering keys, all component fields appear in
definition order within the envelope and within the `__range_start`/`__range_end`
structs.

### Parquet footer metadata

Every delta file carries four key-value metadata entries in the Parquet footer
(written by `delta_parquet.rs`):

| Key | Value |
|---|---|
| `cqlite.delta.version` | `"1"` |
| `cqlite.delta.source` | SSTable identity/generation string (e.g. `"nb-5-big-Data.db"`) |
| `cqlite.delta.schema_hash` | FNV-1a 64-bit hex of the canonical CQL schema (16 chars) |
| `cqlite.version` | CQLite crate version |

Consumers should use `cqlite.delta.source` for deduplication and ordering, and
`cqlite.delta.schema_hash` to detect schema changes across generations.

---

## The five `__op` shapes

### 1. `upsert`

```
__op = 'upsert'
pk, ck: both populated
__ts: null if row was created by UPDATE (no row liveness info);
      non-null (row liveness timestamp) if created by INSERT
Cell structs: non-null structs carry live or tombstoned cell values;
              null struct = column absent in this delta (partial UPDATE)
```

**Merge key:** `(pk, ck)` — per-primary-key last-write-wins.

**Semantics:** Represents any write to a specific row — an INSERT, an UPDATE, or
a cell-level DELETE. Cell columns in this record are the delta for that column
only; absent (null-struct) columns are not touched by this write.

Last-write-wins is applied **per cell**: for a given `(pk, ck)`, the live value
of each column is the `value` from the cell struct with the highest `writetime`,
across all delta files that contain a non-null struct for that column.

When the winning cell struct has `value = null`, the cell is a tombstone —
the column should be treated as deleted (NULL) in the merged view.

### 2. `static_upsert`

```
__op = 'static_upsert'
pk: populated
ck: null (partition-scoped)
__ts: null
Cell structs: non-null structs only for static columns; regular column structs are null
```

**Merge key:** `(pk)` — static columns are per-partition.

**Semantics:** A write to one or more static columns. Since static columns belong
to the partition (not to a specific row), the merge key is `pk` only. LWW per
static cell applies exactly as for regular cells.

### 3. `row_delete`

```
__op = 'row_delete'
pk, ck: both populated (identifies the specific row)
__ts: non-null — deletion timestamp (markedForDeleteAt), µs since epoch
Cell structs: all null (no payload — the whole row is deleted)
```

**Merge key:** `(pk, ck)` — same as `upsert`.

**Semantics:** A `DELETE FROM t WHERE pk=... AND ck=...`. The row identified by
`(pk, ck)` with all cells written at or before `__ts` is deleted. If a later
`upsert` arrives for the same `(pk, ck)` with a cell `writetime > __ts`, that
upsert wins and the row is live with those cells.

### 4. `range_delete`

```
__op = 'range_delete'
pk: populated (partition key)
ck: null (partition-scoped)
__ts: non-null — deletion timestamp
__range_start: Struct { ck: <value|null>, ..., inclusive: bool } or null (open lower bound)
__range_end:   Struct { ck: <value|null>, ..., inclusive: bool } or null (open upper bound)
Cell structs: all null
```

**Merge key:** Applied as a predicate: `(pk)` plus the ck range.

**Semantics:** A `DELETE FROM t WHERE pk=... AND ck >= ... AND ck < ...` (or
similar range). All rows in the partition `pk` whose clustering key falls within
`[__range_start, __range_end]` and whose cells have `writetime <= __ts` are
deleted.

Range bounds are **prefix-aware**: if a table has compound clustering keys
`(year INT, month INT)`, a range-delete on year only has `year` populated in the
bound structs with `month = null`; the null trailing component means "any value
of month".

Bound inclusivity is expressed separately per bound via the `inclusive` field:
- `inclusive = true` means `ck >= bound` (or `ck <= bound` for end)
- `inclusive = false` means `ck > bound` (or `ck < bound` for end)
- A null `__range_start` struct means the range has no lower bound (open).
- A null `__range_end` struct means the range has no upper bound (open).

### 5. `partition_delete`

```
__op = 'partition_delete'
pk: populated (identifies the partition)
ck: null
__ts: non-null — deletion timestamp
__range_start, __range_end: both null
Cell structs: all null
```

**Merge key:** Applied as a predicate over `(pk)`.

**Semantics:** A `DELETE FROM t WHERE pk=...` — the entire partition identified
by `pk` is deleted at `__ts`. All rows in the partition with cells written at or
before `__ts` are deleted. Later upserts for the same `pk` with `writetime > __ts`
survive.

---

## Null-struct vs. `{value: null, writetime}` — the critical distinction

The envelope uses two different null representations, and consumers must not
conflate them:

| Representation | Meaning |
|---|---|
| Null cell struct (top-level null) | **Column absent** — this delta did not touch this column. No action; preserve whatever value came from an earlier delta. |
| Non-null struct with `value = null` | **Cell tombstone** — this delta explicitly deleted this cell. The column should appear as NULL in the merged view. |

Example from the design doc:

```
-- UPDATE t SET val='x' WHERE pk=1 AND ck='a'  (partial update, st not touched)
{ pk:1, ck:'a', __op:'upsert', __ts:null,
  val:{value:'x', writetime:t1, expires_at:null},   -- val is written
  st:null }                                          -- st is ABSENT (struct-level null)

-- DELETE val FROM t WHERE pk=1 AND ck='a'  (cell tombstone)
{ pk:1, ck:'a', __op:'upsert', __ts:null,
  val:{value:null, writetime:t2, expires_at:null},  -- val is TOMBSTONED (null value inside struct)
  st:null }
```

When building a merged view, a null-struct column should simply be skipped (no
write to that column in this delta). A non-null struct with `value = null` should
overwrite any earlier live value with NULL — subject to `writetime` winning LWW.

---

## TTL and `expires_at` filtering

When a cell is written with a TTL (e.g. `INSERT INTO t (...) VALUES (...) USING TTL 3600`),
the `expires_at` field in the cell struct carries the expiration time as
microseconds since epoch. `expires_at = null` means no TTL.

CQLite **never** resolves TTL expiration at scan time — the envelope always
carries the raw `expires_at` value. This makes the output idempotent: the same
delta file read at different times gives the same bytes.

Consumers that want a "live data only" view must filter at query time:

```sql
-- Include only cells that are either non-expiring or have not yet expired.
-- epoch_us(current_timestamp) returns the current time in microseconds.
WHERE val IS NULL
   OR val.expires_at IS NULL
   OR val.expires_at > epoch_us(current_timestamp)
```

In the reference merge below, this filter is applied to each cell column in the
`CASE` expression that extracts the final value.

---

## Collection `replaced` semantics and v1 limitations

Non-frozen collection columns (`list`, `set`, `map`) carry an additional field in
their cell struct:

```
col : Struct (nullable) {
  value:     List<T> | Map<K,V>,  -- elements present in this delta
  writetime: Int64,
  expires_at: Int64 (nullable),
  replaced:   Boolean,            -- collections only: true = replace, false = merge
}
```

The `replaced` flag distinguishes two write shapes:

| `replaced` | CQL statement | Merge action |
|---|---|---|
| `false` | `UPDATE t SET s = s + {'x'}` (append) | **Merge** — add the elements in `value` to the existing set/list/map; do not remove existing elements |
| `true` | `UPDATE t SET s = {'x'}` (overwrite) | **Replace** — discard all previously seen elements for this column; use `value` as the new complete state |

### V1 limitation: element-level removals

CQLite v1 does NOT represent individual element removals from collections (e.g.
`UPDATE t SET s = s - {'old_element'}`). These writes are detected and counted
in the scan summary's warning counter, but they do not appear as removal events
in the delta envelope.

Practical consequence: if your data has element-level set/list/map removals,
the merged collection in the downstream view may contain stale elements that were
deleted in Cassandra. The warning counter in the scan summary tells you how many
such removals were encountered.

Full element-level fidelity is tracked in issue #493 and is planned for a future
version. See also Epic #696 which tracks the full delta-scan epic.

### Collection merge in SQL

The reference SQL below shows the `replaced` flag approach for a single
collection column `tags set<text>`. Full element-merge across N generations
requires user-side list aggregation and de-duplication because SQL's
`UNION`-of-arrays semantics vary by engine.

---

## DuckDB SQL reference merge

This section contains a complete, copy-paste DuckDB SQL merge over N per-generation
delta Parquet files. The DS11 round-trip test (Issue #707) executes this exact SQL
against real delta files and asserts the result equals `cqlite SELECT *` output.

### Schema

Table used for all examples:
```sql
-- t (pk INT, ck TEXT, val TEXT, st TEXT STATIC, PRIMARY KEY (pk, ck))
```

### Reading multiple delta files

```sql
-- Read all delta files for one table (glob by generation pattern).
-- Replace the path glob with your actual file location.
CREATE OR REPLACE VIEW all_deltas AS
SELECT * FROM read_parquet('/path/to/deltas/my_ks/t/nb-*-Data.db.delta.parquet');
```

Each file produced by `cqlite delta-export` is one generation. DuckDB's
`read_parquet` with a glob unions all files transparently.

### Complete reference merge

The merge proceeds in four steps, which are expressed as a chain of CTEs:

```sql
-- =============================================================================
-- CQLite delta-scan reference merge (DuckDB)
-- Table: t (pk INT, ck TEXT, val TEXT, st TEXT STATIC, PRIMARY KEY (pk, ck))
--
-- Verified against DuckDB v1.5.3 (Variegata). Field access paths match the
-- real Arrow schema emitted by cqlite-core/src/export/delta_schema.rs and
-- delta_parquet.rs.
-- =============================================================================

-- Step 1: Determine the effective deletion time per (pk, ck) from row_delete ops.
-- A row_delete at timestamp T deletes all cells written at or before T.
WITH row_delete_hwm AS (
    SELECT
        pk,
        ck,
        MAX(__ts) AS del_ts          -- highest deletion timestamp seen for this row
    FROM all_deltas
    WHERE __op = 'row_delete'
    GROUP BY pk, ck
),

-- Step 2: Determine the effective deletion time per pk from partition_delete ops.
partition_delete_hwm AS (
    SELECT
        pk,
        MAX(__ts) AS del_ts          -- highest deletion timestamp seen for this partition
    FROM all_deltas
    WHERE __op = 'partition_delete'
    GROUP BY pk
),

-- Step 3: Determine the effective deletion time per pk from range_delete ops
-- for each row (matched by ck falling within the range bounds).
-- This CTE joins rows against range_delete records in the same partition.
-- For multi-component clustering keys, extend the bound checks to all components.
range_delete_hwm AS (
    SELECT
        u.pk,
        u.ck,
        MAX(rd.__ts) AS del_ts
    FROM all_deltas u
    -- Cross all range_delete records in the same partition
    JOIN all_deltas rd
      ON rd.__op = 'range_delete'
     AND rd.pk = u.pk
     -- Lower bound check (null __range_start = open lower bound)
     AND (rd.__range_start IS NULL
          OR (rd.__range_start.inclusive     AND u.ck >= rd.__range_start.ck)
          OR (NOT rd.__range_start.inclusive AND u.ck >  rd.__range_start.ck))
     -- Upper bound check (null __range_end = open upper bound)
     AND (rd.__range_end IS NULL
          OR (rd.__range_end.inclusive     AND u.ck <= rd.__range_end.ck)
          OR (NOT rd.__range_end.inclusive AND u.ck <  rd.__range_end.ck))
    WHERE u.__op IN ('upsert', 'row_delete')
    GROUP BY u.pk, u.ck
),

-- Step 4a: LWW merge for the regular column `val` across all upsert records.
-- For each (pk, ck), keep only the cell struct with the highest writetime.
-- The cell struct may have value=null (tombstone) or value=<text> (live).
-- Null struct = column absent in this delta = skip (do not overwrite).
val_lww AS (
    SELECT
        pk,
        ck,
        val.value     AS val_value,      -- null if cell tombstone
        val.writetime AS val_writetime,
        val.expires_at AS val_expires_at
    FROM all_deltas
    WHERE __op = 'upsert'
      AND val IS NOT NULL                 -- skip absent-column records
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY pk, ck
        ORDER BY val.writetime DESC       -- highest writetime wins
    ) = 1
),

-- Step 4b: LWW merge for the static column `st` across all static_upsert records.
-- Merge key is pk only (static columns are per-partition).
st_lww AS (
    SELECT
        pk,
        st.value      AS st_value,
        st.writetime  AS st_writetime,
        st.expires_at AS st_expires_at
    FROM all_deltas
    WHERE __op = 'static_upsert'
      AND st IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY pk
        ORDER BY st.writetime DESC
    ) = 1
),

-- Step 5: Assemble the final merged view.
-- Combine LWW cell values, apply delete suppression, and filter expired TTL cells.
final AS (
    SELECT
        v.pk,
        v.ck,
        -- val: null if tombstoned (val_value IS NULL), expired, or deleted
        CASE
            WHEN v.val_value IS NULL THEN NULL            -- cell tombstone
            WHEN v.val_expires_at IS NOT NULL
             AND v.val_expires_at <= epoch_us(current_timestamp) THEN NULL  -- TTL expired
            ELSE v.val_value
        END AS val,

        -- st: joined from static LWW (may be null if no static write in these generations)
        CASE
            WHEN s.st_value IS NULL THEN NULL
            WHEN s.st_expires_at IS NOT NULL
             AND s.st_expires_at <= epoch_us(current_timestamp) THEN NULL
            ELSE s.st_value
        END AS st

    FROM val_lww v
    LEFT JOIN st_lww s ON s.pk = v.pk

    -- Suppress rows killed by a row_delete with del_ts >= the winning writetime.
    WHERE NOT EXISTS (
        SELECT 1 FROM row_delete_hwm rd
        WHERE rd.pk = v.pk
          AND rd.ck = v.ck
          AND rd.del_ts >= v.val_writetime  -- delete timestamp at or after cell write
    )
    -- Suppress rows killed by a partition_delete.
    AND NOT EXISTS (
        SELECT 1 FROM partition_delete_hwm pd
        WHERE pd.pk = v.pk
          AND pd.del_ts >= v.val_writetime
    )
    -- Suppress rows killed by a range_delete.
    AND NOT EXISTS (
        SELECT 1 FROM range_delete_hwm rg
        WHERE rg.pk = v.pk
          AND rg.ck = v.ck
          AND rg.del_ts >= v.val_writetime
    )
)

SELECT pk, ck, val, st
FROM final
ORDER BY pk, ck;
```

### Key access paths (citing `delta_schema.rs`)

Every struct field access in the SQL above uses the exact field names emitted by
`cqlite-core/src/export/delta_schema.rs`:

| SQL expression | Arrow field path | Source |
|---|---|---|
| `val.value` | `val → value` (nullable) | `build_cell_struct_field`: field named `"value"` |
| `val.writetime` | `val → writetime` (Int64) | `build_cell_struct_field`: field named `"writetime"` |
| `val.expires_at` | `val → expires_at` (nullable Int64) | `build_cell_struct_field`: field named `"expires_at"` |
| `__range_start.ck` | `__range_start → ck` (nullable) | `build_range_bound_field`: field named for the clustering column |
| `__range_start.inclusive` | `__range_start → inclusive` (Boolean) | `build_range_bound_field`: field named `"inclusive"` |
| `__ts` | top-level `Int64` column | `DeltaSchemaOpts::ts_col()` → `"__ts"` |
| `__op` | top-level `Dictionary(Int8, Utf8)` column | `DeltaSchemaOpts::op_col()` → `"__op"` |

### Collection column merge (`replaced` flag)

For a collection column `tags SET<TEXT>`, the cell struct has an additional
`replaced: Boolean` field (see `delta_schema.rs: build_cell_struct_field`):

```sql
-- Collection merge CTE (replace-or-merge based on `replaced` flag).
-- For each (pk, ck), produce one tags_parts record per generation delta
-- that touched the column, ordered by writetime.
tags_merge AS (
    SELECT
        pk,
        ck,
        tags.value      AS tags_elements,   -- List<Utf8> of elements in this delta
        tags.writetime  AS tags_writetime,
        tags.replaced   AS tags_replaced    -- true = replace, false = append
    FROM all_deltas
    WHERE __op = 'upsert'
      AND tags IS NOT NULL
    ORDER BY pk, ck, tags.writetime
)
-- Consumers must implement merge vs replace logic in their preferred language or
-- by iterating in writetime order:
--   - When tags_replaced = true: discard all prior elements, start fresh with tags_elements.
--   - When tags_replaced = false: add tags_elements to the accumulated set.
-- Note: v1 does NOT represent individual element removals; see §V1 Limitations.
```

DuckDB does not have a built-in "accumulate with reset" aggregate, so collection
merge is typically done with a small Python/JS/Rust post-pass or a UDF. For
append-only workloads (no overwrites, `replaced` always false), `list_distinct(flatten(list_agg(...)))` works:

```sql
-- Append-only collection merge (no overwrites, tags_replaced always false)
SELECT pk, ck, list_distinct(flatten(list_agg(tags_elements))) AS tags
FROM tags_merge
WHERE NOT tags_replaced      -- guard: only safe if all writes are appends
GROUP BY pk, ck;
```

---

## Worked examples

These follow the six examples from the design doc
(`docs/plans/2026-06-10-delta-scan-envelope-design.md`).

### Example 1: Partial UPDATE (upsert, val written, st absent)

```
{ pk:1, ck:'a', __op:'upsert', __ts:null,
  val:{value:'x', writetime:t1, expires_at:null}, st:null }
```

- `val` is non-null struct, `val.value = 'x'`, `val.writetime = t1`.
- `st` is null struct (absent — not touched by this UPDATE).
- Merge: `val = 'x'` for `(pk=1, ck='a')`; `st` unchanged from any prior delta.

### Example 2: Cell tombstone (DELETE val FROM t ...)

```
{ pk:1, ck:'a', __op:'upsert', __ts:null,
  val:{value:null, writetime:t2, expires_at:null}, st:null }
```

- `val` is non-null struct (cell is present in this delta).
- `val.value` is null — this is a **cell tombstone**, not an absent column.
- If `t2 > t1`, the merge result for `val` at `(pk=1, ck='a')` is `NULL`.

### Example 3: Static write (UPDATE t SET st='S' WHERE pk=1)

```
{ pk:1, ck:null, __op:'static_upsert',
  st:{value:'S', writetime:t3, expires_at:null}, val:null }
```

- `ck` is null (partition-scoped).
- `st` is non-null struct, `st.value = 'S'`.
- `val` is null struct (absent — static write does not touch regular columns).
- Merge: `st = 'S'` for all rows in partition `pk=1`.

### Example 4: Row tombstone (DELETE FROM t WHERE pk=1 AND ck='a')

```
{ pk:1, ck:'a', __op:'row_delete', __ts:t4, val:null, st:null }
```

- `__ts = t4` is the deletion timestamp.
- All cells for `(pk=1, ck='a')` with `writetime <= t4` are deleted.
- An upsert at `(pk=1, ck='a')` with `writetime > t4` survives.

### Example 5: Range delete (DELETE FROM t WHERE pk=1 AND ck >= 'a' AND ck < 'm')

```
{ pk:1, ck:null, __op:'range_delete', __ts:t5,
  __range_start:{ck:'a', inclusive:true},
  __range_end:{ck:'m', inclusive:false} }
```

- `__ts = t5` is the deletion timestamp.
- All rows in partition `pk=1` with `ck >= 'a' AND ck < 'm'` and `writetime <= t5` are deleted.
- A row at `ck='c'` with `writetime <= t5` is deleted; at `writetime > t5` it survives.
- A row at `ck='m'` is outside the range (end is exclusive) and is not deleted.

### Example 6: Partition tombstone (DELETE FROM t WHERE pk=1)

```
{ pk:1, __op:'partition_delete', __ts:t6 }
```

- `__ts = t6` is the deletion timestamp.
- All rows in partition `pk=1` with `writetime <= t6` are deleted.
- A later upsert for `(pk=1, ck='z')` with `writetime > t6` survives.

---

## `__ts` and last-write-wins rules

The `__ts` field is the record-level timestamp. Its role differs by `__op`:

| `__op` | `__ts` meaning |
|---|---|
| `upsert` | Row liveness timestamp (null if created by UPDATE without INSERT liveness). Per-cell LWW uses `cell.writetime`, not `__ts`. |
| `static_upsert` | Always null. Use `cell.writetime` for LWW. |
| `row_delete` | Deletion timestamp — cells with `writetime <= __ts` are deleted. |
| `range_delete` | Deletion timestamp — cells in range with `writetime <= __ts` are deleted. |
| `partition_delete` | Deletion timestamp — all cells with `writetime <= __ts` are deleted. |

Summary: **cell-level writes use `cell.writetime` for LWW; delete ops use `__ts`
as the "all cells up to this time" watermark.**

A delete op at `__ts = T` is overridden by a later upsert with `cell.writetime > T`.
A delete op at `__ts = T` overrides an earlier upsert with `cell.writetime <= T`.

---

## V1 limitations

1. **Collection element removals not represented.** Individual element deletions
   from non-frozen collections (e.g. `s = s - {'old'}`) are detected at scan
   time and counted in the scan summary's warning counter, but they do not
   appear as removal events in the delta envelope. The `value` field carries only
   the elements present in this generation, not the removed elements. This means
   a downstream merge may retain elements that were removed in Cassandra. Tracked
   in issue #493; full element-level fidelity is a planned follow-up in
   Epic #696.

2. **Counter tables rejected at schema derivation.** Tables with `counter` columns
   cannot be projected to the delta envelope. The writer raises an error at
   schema-derivation time (before any output is written).

3. **Cross-SSTable merge is the consumer's job.** CQLite produces one file per
   generation with no GC-grace filtering and no cross-generation merge. The
   consumer is responsible for reading all relevant generations and applying the
   merge recipe in this document.

4. **`duration` columns serialize as `Utf8`.** The `parquet` crate v53 lacks
   `IntervalMonthDayNano` write support; `duration` values appear as CQL text
   strings (e.g. `"1mo2d3ns"`) until upstream support lands.

---

## DuckDB validation output

The reference SQL in this document was validated against DuckDB v1.5.3
(Variegata) on 2026-06-20 using in-memory tables that mirror the real envelope
schema. Key assertions verified:

- `val.value`, `val.writetime`, `val.expires_at` struct field access works.
- `__range_start.ck`, `__range_start.inclusive` struct field access works.
- LWW merge via `QUALIFY ROW_NUMBER() OVER (PARTITION BY pk,ck ORDER BY val.writetime DESC) = 1` produces the highest-writetime row.
- `row_delete` with `del_ts >= val_writetime` correctly suppresses upsert rows.
- `range_delete` predicate (`ck >= start.ck AND ck < end.ck` with `inclusive` checks) correctly suppresses rows in range and preserves rows outside.
- TTL `expires_at` filter correctly suppresses expired cells.

All field names are verified against the Arrow schema produced by
`cqlite-core/src/export/delta_schema.rs:build_cell_struct_field` and
`build_range_bound_field`.
