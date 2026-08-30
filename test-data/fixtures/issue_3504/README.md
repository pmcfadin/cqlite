# issue_3504 — colliding UDT field names (`_type` / `_keyspace`)

A **Cassandra 5.0.2-written** SSTable whose UDT declares fields literally named
`_type` and `_keyspace`. Issue #3504: the bindings rendered a UDT as one flat
namespace holding both the injected type identity and the declared fields, so
such a field silently overwrote the marker. No other fixture in the corpus
declares one, so the defect had no test subject.

## Why it lives here and not in `test-data/datasets/sstables/`

`bindings/python/tests/conftest.py` and `bindings/node/__test__/setup.js` resolve
the dataset corpus from `CQLITE_DATASETS_ROOT` and **never fall back to the
checkout**, so anything committed under `test-data/datasets/sstables/` is
invisible on a box where that env var is set — i.e. on every gate run. This
directory is checkout-relative, so no env var can hide it. Precedent:
`cqlite-core/tests/fixtures/issue_2225/`.

## Layout

This directory **is** an "sstables root": it directly contains the keyspace
directory, exactly like `$CQLITE_DATASETS_ROOT/sstables`. Open it the way the
dataset tests open that root and query `test_udt_collision.udt_collide`.

```
test-data/fixtures/issue_3504/
└── test_udt_collision/
    └── udt_collide-262cf840a4a011f193e23181f4b17b37/
        ├── nb-1-big-Data.db            (+ Index/Summary/Filter/Statistics/CRC/Digest/TOC)
        ├── nb-1-big-Data.db.jsonl      sstabledump golden (-l, one partition per line)
        └── nb-1-big-Statistics.db.txt  sstablemetadata dump
```

Uncompressed (no `CompressionInfo.db`), BIG `nb`, ~350-byte `Data.db`.

## Schema and regeneration

- Schema: `test-data/schemas/issue-3504-udt-collision.cql` (types `collide`,
  `collide_twin`, `plain`; table `udt_collide`).
- Regenerate: `bash test-data/scripts/generate-issue-3504-udt-collision.sh`
  (needs Docker). The `*.db` binaries are gitignored — the script prints the
  `git add -f` lines you must use after a regeneration. A regeneration produces a
  NEW table-directory UUID, so any test that hardcodes the path must be updated;
  prefer globbing `test_udt_collision/udt_collide-*/`.

## Contents (all rows at `USING TIMESTAMP 1000`)

| id | `c frozen<collide>` | `p frozen<plain>` | `cm map<frozen<collide>,int>` | `tm map<frozen<collide_twin>,int>` |
|----|---------------------|-------------------|-------------------------------|------------------------------------|
| 1 | `_type='user-supplied-type'`, `_keyspace='user-supplied-keyspace'`, `real_field=42` | `label='no-colliding-field'`, `real_field=7` | key `_type='key-type-marker'`, `_keyspace='key-keyspace-marker'`, `real_field=100` → 1 | same field values, different TYPE → 2 |
| 2 | null | `label='contrast-row'`, `real_field=8` | null | null |
| 3 | `_type=NULL`, `_keyspace='keyspace-field-only'`, `real_field=0` | null | null | null |

What each is for:

- **id 1 `c`** — the site-3 subject: all three fields populated with distinct,
  recognizable values, so an overwrite is *visible* rather than merely absent.
- **id 1 `cm`** — the site-4 subject: a UDT as a **map key**, which the Python
  binding must project to a hashable value.
- **id 1 `tm`** — `collide_twin` has the same three field names and the same
  values as the `cm` key but a **different type name**, so a projection that
  drops type identity collapses the two keys while one that keeps it does not.
  Note the sstabledump golden renders both cell paths identically
  (`key-type-marker:key-keyspace-marker:100`) — type identity is genuinely not
  in the bytes of the cell path, it comes from the column's declared type.
- **id 2 `p`** — the non-colliding contrast: a UDT with no `_type` field at all,
  where reading `_type` out of the field namespace must fail.
- **id 3 `c`** — a NULL colliding field, pinning that a frozen UDT's
  absent-field encoding is orthogonal to the collision.
