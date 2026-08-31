# issue_3630 — row-level `Object.prototype` collision fixture

Cassandra-**5.0.2-written** SSTables for issue #3630: the Node binding writes user-controlled
column names onto a plain JavaScript object through an ordinary `[[Set]]`, which consults the
prototype chain. Schema: `test-data/schemas/issue-3630-row-collision.cql` (read it for the
per-column rationale). Generator: `test-data/scripts/generate-issue-3630-row-collision.sh`.

Committed **checkout-relative** and NOT under `test-data/datasets/sstables/`: both binding suites
resolve the corpus from `CQLITE_DATASETS_ROOT` and never fall back to the checkout, so a
corpus-rooted fixture is invisible on exactly the runs that must see it. The fixture root is itself
an sstables root — it directly contains the keyspace directory.

`test_row_collision.row_collide`, one SSTable (`nb-1-big-*`), three partitions.

## MEASURED on the generated golden — read this before writing an assertion

Two findings from `nb-1-big-Data.db.jsonl` that change what this fixture can prove. Both were
measurements, not predictions, and one of them contradicts the generator's first draft.

### 1. Row 2's NULL `"__proto__"` is a CELL TOMBSTONE, not a null VALUE

```json
{"name": "\"__proto__\"", "deletion_info": {"local_delete_time": "2026-08-31T03:57:58Z"}}
```

An explicit CQL `null` in an INSERT writes a cell tombstone — there is **no value cell**. So:

- **This row very likely CANNOT exhibit the prototype-replacement failure mode through the Node
  binding.** That mode needs an actual `null` to be ASSIGNED to `__proto__`; if the decoder yields
  no entry for the column, `row_to_object` **skips** it (a metadata column with no matching value is
  skipped, never null-filled — the #1446 contract) and no assignment happens at all.
- **The prototype-replacement oracle must therefore be a Rust-level unit test** over
  `row_to_object` with a value map explicitly containing `Value::Null`. The consuming test MUST
  RECORD which behaviour it observed at the binding surface rather than asserting the expectation
  blind.
- **Row 2's real value is a different requirement**: it is a column DECLARED in metadata with NO
  value, i.e. the skip path. That contract must survive this change, and row 2 is what pins it.

### 2. ONE FIELD OF THIS GOLDEN IS NOT REPRODUCIBLE, and the generator's header overstated this

Every INSERT pins `USING TIMESTAMP 1000`, so `liveness_info.tstamp` is stable. But the row-2 cell
tombstone carries `local_delete_time`, a **wall clock** (`nowInSeconds`) that **no CQL clause can
pin**. A regeneration will therefore differ in exactly that one field.

This is the same residual `test-data/schemas/issue-3504-udt-collision.cql` records for its
non-frozen map collection tombstones. It is recorded here rather than quietly tolerated because the
generator's first draft claimed the golden was reproducible full stop, which was **false for row
2** — and an overclaim in a committed fixture is worse than a stated limitation, since it is what
stops the next reader checking.

Consequence: do **not** byte-compare this golden across regenerations. Compare the value cells, or
normalise `local_delete_time` away first.

### 3. sstabledump quotes only the names CQL would require quoting

`"\"__proto__\""` and `"\"toString\""` appear quoted in the golden; `constructor` and `prototype`
appear **bare**. That is Cassandra's identifier round-tripping rule — a leading underscore or mixed
case needs quoting, an all-lowercase leading-letter name does not — and it is a property of the
DUMP, not of the stored name. A test that greps the golden must expect both spellings; a test that
reads column names through CQLite's metadata should expect the unquoted logical names.

## Rows

| id | `"__proto__"` | `"constructor"` | `"toString"` | `"prototype"` | `real_col` | role |
|---|---|---|---|---|---|---|
| 1 | `user-supplied-proto` | set | set | set | 42 | the STRING case — the column that vanishes unfixed |
| 2 | **cell tombstone** (explicit CQL null) | set | set | set | 43 | metadata-declared, no value ⇒ the SKIP path (see finding 1) |
| 3 | absent | absent | absent | absent | 44 | contrast case — shaped identically before and after |
