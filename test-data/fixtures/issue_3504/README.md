# issue_3504 — colliding UDT field names (`_type` / `_keyspace`)

A **Cassandra 5.0.2-written** SSTable whose UDT declares fields literally named
`_type` and `_keyspace`. Issue #3504: the bindings rendered a UDT as one flat
namespace holding both the injected type identity and the declared fields, so
such a field silently overwrote the marker. No other fixture in the corpus
declares one, so the defect had no test subject.

## Why it lives here and not in `test-data/datasets/sstables/`

`bindings/python/tests/conftest.py` (`:42-48`) and `bindings/node/__test__/setup.js`
(`:23`) resolve the dataset corpus as an **either/or on `CQLITE_DATASETS_ROOT`**:
with the variable unset they *do* use the checkout's `test-data/datasets`. But
**when it is set — which every gate run does — the checkout copy is never
consulted**, so anything committed under `test-data/datasets/sstables/` is
invisible exactly where these suites run. This
directory is checkout-relative, so no env var can hide it. Precedent:
`cqlite-core/tests/fixtures/issue_2225/`.

## What the JSONL golden IS and IS NOT an oracle for

The committed `*-Data.db.jsonl` (`sstabledump -l`) pins **decode** and proves the
colliding schema is legal Cassandra. It is **not** an oracle for the binding
rendering rule: for this input, sstabledump's flat
`{"_type":"user-supplied-type", …}` is textually identical to what the OLD buggy
binding injection produced, so physical-dump parity is structurally blind to the
defect. Do not cite dump parity as evidence that the rendering is fixed — the
oracle for that is the binding-level assertion on `.type_name`/`.fields`.

## Layout

This directory **is** an "sstables root": it directly contains the keyspace
directory, exactly like `$CQLITE_DATASETS_ROOT/sstables`. Open it the way the
dataset tests open that root and query `test_udt_collision.udt_collide`.

```
test-data/fixtures/issue_3504/
└── test_udt_collision/
    └── udt_collide-dd179970a4a011f1b1b73181f4b17b37/
        ├── nb-1-big-Data.db            (+ Index/Summary/Filter/Statistics/CRC/Digest/TOC)
        ├── nb-1-big-Data.db.jsonl      sstabledump golden (-l, one partition per line)
        └── nb-1-big-Statistics.db.txt  sstablemetadata dump
```

Uncompressed (no `CompressionInfo.db`), BIG `nb`, ~500-byte `Data.db`.

## Schema and regeneration

- Schema: `test-data/schemas/issue-3504-udt-collision.cql` (types `collide`,
  `collide_twin`, `plain`; table `udt_collide`).
- Regenerate: `bash test-data/scripts/generate-issue-3504-udt-collision.sh`
  (needs Docker). The `*.db` binaries are gitignored — the script prints the
  `git add -f` lines you must use after a regeneration. A regeneration produces a
  NEW table-directory UUID, so **glob** `test_udt_collision/udt_collide-*/`
  rather than hardcoding the path.

## Contents (one row per id, all at `USING TIMESTAMP 1000`)

`id 1` populates every column; `id 2` has only `p`; `id 3` has only `c`.

| column | type | id 1 value | what it is for |
|---|---|---|---|
| `c` | `frozen<collide>` | `_type='user-supplied-type'`, `_keyspace='user-supplied-keyspace'`, `real_field=42` | **site 3**: the rendered UDT. Distinct, recognizable values, so an overwrite is *visible* rather than merely absent. |
| `p` | `frozen<plain>` | `label='no-colliding-field'`, `real_field=7` | the non-colliding contrast — a UDT with no `_type` field at all, where reading `_type` out of the field namespace must fail. |
| `cm` | `map<frozen<collide>,int>` | key `_type='key-type-marker'`, `_keyspace='key-keyspace-marker'`, `real_field=100` → 1 | the shape a user would naturally write. **MEASURED: does NOT reach the Python hashable projection** — see below. |
| `tm` | `map<frozen<collide_twin>,int>` | same field values → 2 | the same, one type over. |
| `fcm` | `frozen<map<frozen<collide>,int>>` | same field values → 3 | **site 4's actual subject**: decodes to `Frozen(Map([(Udt{…}, 3)]))`, so the key really is a UDT. |
| `ftm` | `frozen<map<frozen<collide_twin>,int>>` | same field values → 4 | same field NAMES and VALUES as `fcm`'s key under a **different type name**: two projected keys that must stay DISTINCT once type identity leaves the field namespace. |
| `fs` | `frozen<set<frozen<collide>>>` | `_type='set-member-type'`, `_keyspace='set-member-keyspace'`, `real_field=200` | the set path into the same projection (`set_to_py` shares `value_to_hashable_key` with map keys). |

`id 3`'s `c` is `_type=NULL`, `_keyspace='keyspace-field-only'`, `real_field=0` —
a **second, distinct failure mode**: under the old code a null `_type` *field*
overwrote the injected type name with `None`, which is not the same defect as the
string case. Assert `.type_name` is still the real UDT type and that the `_type`
FIELD is `None`.

### Why both a non-frozen and a frozen map column

Measured against the generated fixture: a **non-frozen** `map<frozen<udt>,int>`
is multicell, so its key lives in the CELL PATH, and
`parse_cell_path_key` (`cqlite-core/src/storage/sstable/reader/parsing/row_decoder/complex_column.rs`)
matches a closed set of **primitive** cell-path types and falls back to
`Value::Blob` for a frozen UDT. `cm`/`tm` therefore decode to
`Map([(Blob(<serialized udt bytes>), int)])` and never reach a `Value::Udt`.
A **frozen** map is a single value cell decoded by `parse_map_with_types`, which
resolves the key type through the `UdtRegistry` — so `fcm`/`ftm`/`fs` are the
columns that actually exercise the projection. Both shapes are committed: the
frozen ones as the test subject, the non-frozen ones because they are the natural
user spelling and they document the gap.
