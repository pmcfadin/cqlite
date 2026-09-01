# issue_3504 — colliding UDT field names (`_type` / `_keyspace` / `__proto__`)

A **Cassandra 5.0.2-written** SSTable whose UDT declares fields literally named
`_type`, `_keyspace` and `__proto__`. Issue #3504: the bindings rendered a UDT as
one flat namespace holding both the injected type identity and the declared
fields, so such a field silently overwrote the marker. No other fixture in the
corpus declares one, so the defect had no test subject.

`__proto__` is the same class one layer down, in JavaScript's object model
(roborev R1-1): an ordinary property assignment on a plain object reaches
`Object.prototype`'s inherited `__proto__` accessor, so — measured on THIS
fixture, before the fix — the string-valued field in `id 1` VANISHED from the
rendered object and the null-valued one in `id 3` REPLACED the field bag's
prototype. The Node binding therefore builds `fields` with a null prototype.
Cassandra accepts all three names as quoted identifiers.

The directory holds **two tables**: `udt_collide` (the namespace collision) and
`udt_hashable_shapes` (the Python hashable-projection totality boundary, roborev
R1-2 — see its own section below).

## Why it lives here and not in `test-data/datasets/sstables/`

`bindings/python/tests/conftest.py` (`:42-48`) and `bindings/node/__test__/setup.js`
(`:23`) resolve the dataset corpus as an **either/or on `CQLITE_DATASETS_ROOT`**:
with the variable unset they *do* use the checkout's `test-data/datasets`. But
**when it is set — which every gate run does — the checkout copy is never
consulted**, so anything committed under `test-data/datasets/sstables/` is
invisible exactly where these suites run. This
directory is checkout-relative, so no env var can hide it. Precedent:
`cqlite-core/tests/fixtures/issue_2225/`.

## Who consumes this fixture (all four sites are now FIXED)

| Site | Fixed by | Test |
|---|---|---|
| Python binding (`udt_to_py`, `value_to_hashable_key`) | #3504 | `bindings/python/tests/test_issue_3504_udt_field_namespace.py` |
| Node binding (`udt_to_object`) | #3504 | `bindings/node/__test__/issue-3504-udt-field-namespace.test.js` |
| **CLI `--format json`** (`cqlite-cli/src/output/json.rs`) | **#3629** | `cqlite-cli/tests/issue_3629_cli_udt_json_namespace.rs` |
| **`cqlite-core` `ToJson for Value`** (`src/query/result.rs`) | **#3629** | `cqlite-core/tests/issue_3629_core_tojson_udt_namespace.rs` |

**#3612 changed WHICH COLUMNS reach three of these four sites, without changing
the sites themselves.** Before it, only the FROZEN columns (`fcm`/`ftm`/`fs`)
delivered a structured `Value::Udt`; the multicell `cm`/`tm` keys arrived as an
opaque `Value::Blob` from the cell-path fallback and reached no UDT renderer at
all. They now decode structurally, so `cm`/`tm` additionally exercise the two
bindings and the CLI's `--format json` — a second, MULTICELL route into the same
renderers. `cqlite-core`'s `ToJson` is the exception and stays a non-subject for
these columns, because its `Map` arm `Display`-stringifies every key regardless of
type.

The two #3629 sites were an independent SECOND COPY of the same defect and are now
one shared rule, `cqlite-core/src/util/udt_json.rs::udt_to_json_object` — declared
fields and nothing else, generic over the field-VALUE renderer because the two
writers deliberately differ elsewhere (hex vs base64 blobs, human vs raw
timestamps, `[{key,value}]` vs Display-keyed maps). Consequence, by design:
`--format json` carries **no type channel at all**, exactly like `sstabledump`.

The rule's primary source is Cassandra, not this repo:
`cassandra-5.0.8:src/java/org/apache/cassandra/db/marshal/UserType.java:261`
(`toJSONString`) iterates `types.size()` over `stringFieldNames` alone — no type key, no
keyspace key — and appends the literal `null` for an absent field buffer (line 280), which is
why `id 3`'s `"_type": null` below is CORRECT output for a null field.

## What the JSONL golden IS and IS NOT an oracle for

The committed `*-Data.db.jsonl` (`sstabledump -l`) pins **decode** and proves the
colliding schema is legal Cassandra. It is **not** an oracle for the binding
rendering rule: for this input, sstabledump's flat
`{"_type":"user-supplied-type", …}` is textually identical to what the OLD buggy
binding injection produced, so physical-dump parity is structurally blind to the
defect. Do not cite dump parity as evidence that the rendering is fixed — the
oracle for that is the binding-level assertion on `.type_name`/`.fields`.

**Which columns can go RED, measured (#3629).** The blindness above is per COLUMN,
and getting it wrong produces a test that passes on unfixed code:

* **RED-capable** — the UDTs that declare no `_type` field, where an injected key
  is observable: `udt_collide.p` (`frozen<plain>`; golden
  `{"label": "no-colliding-field", "real_field": 7}`) and
  `udt_hashable_shapes.stn`'s `unhashable_fields`, nested in a tuple in a set
  (golden `[[{"label": "unhashable", "m": {"a": 1}}, 30]]`).
* **BLIND** — every `collide`/`collide_twin` column (`c`, `fs`, `fcm`, `ftm`,
  `stu`, `ssu`, `mtu`, and since #3612 `cm`/`tm` too — third bullet): the user's
  `_type` field TOTALLY overwrites the injected
  one, and because `serde_json` is built with `preserve_order` and `Map::insert`
  keeps an existing key's position while `collide` declares `"_type"` first, even
  the key ORDER matched. Assert these as preservation guards only, labelled as
  such. `id 3`'s `"_type": null` is the USER's null field and is CORRECT.
* **RECLASSIFIED BY #3612** — `cm`/`tm`. This bullet read **NOT A SUBJECT AT
  ALL**, and the reasoning was sound for the code of the time: a non-frozen map's
  key lives in the cell path, `parse_cell_path_key` fell back to `Value::Blob`,
  so the key never became a `Value::Udt` and could not exercise a UDT renderer.
  That is now HISTORY. The site delegates to the structural decoder (see the
  #3612 note above), so these keys DO reach both bindings and the CLI's
  `--format json`. They join the **BLIND** class rather than the RED-capable one,
  for the second bullet's reason and not a new one: their key type is
  `collide`/`collide_twin`, which DECLARES `_type`, so the user's field totally
  overwrites any injected marker. `cqlite-core`'s `ToJson` stays a non-subject
  for them, because its `Map` arm `Display`-stringifies every key regardless of
  type.

## Layout

This directory **is** an "sstables root": it directly contains the keyspace
directory, exactly like `$CQLITE_DATASETS_ROOT/sstables`. Open it the way the
dataset tests open that root and query `test_udt_collision.udt_collide`.

```
test-data/fixtures/issue_3504/
├── binding-parity-facts.json           the CROSS-BINDING reference (see below)
└── test_udt_collision/
    ├── udt_collide-<uuid>/
    │   ├── nb-1-big-Data.db            (+ Index/Summary/Filter/Statistics/CRC/Digest/TOC)
    │   ├── nb-1-big-Data.db.jsonl      sstabledump golden (-l, one partition per line)
    │   └── nb-1-big-Statistics.db.txt  sstablemetadata dump
    └── udt_hashable_shapes-<uuid>/     same component set
```

Uncompressed (no `CompressionInfo.db`), BIG `nb`, ~500-byte `Data.db`.

`binding-parity-facts.json` is committed TEXT, not a binary: it records the UDT
facts (`typeName`/`keyspace`/`fields`) and map values that BOTH bindings must
produce from row `id 1`. The Python and Node suites each derive that fact set
from their OWN binding output and assert equality against this one file, so the
two surfaces are compared as DATA and cannot drift apart independently. It is
resolved CHECKOUT-RELATIVE for the same reason the binaries are — see its
`note_on_paths`.

## Schema and regeneration

- Schema: `test-data/schemas/issue-3504-udt-collision.cql` (types `collide`,
  `collide_twin`, `plain`, `unhashable_fields`; tables `udt_collide` and
  `udt_hashable_shapes`).
- Regenerate: `bash test-data/scripts/generate-issue-3504-udt-collision.sh`
  (needs Docker). The `*.db` binaries are gitignored — the script prints the
  `git add -f` lines you must use after a regeneration. A regeneration produces a
  NEW table-directory UUID, so **glob** `test_udt_collision/udt_collide-*/`
  rather than hardcoding the path (for both tables).

## Contents (one row per id, all at `USING TIMESTAMP 1000`)

`id 1` populates every column; `id 2` has only `p`; `id 3` has only `c`.

| column | type | id 1 value | what it is for |
|---|---|---|---|
| `c` | `frozen<collide>` | `_type='user-supplied-type'`, `_keyspace='user-supplied-keyspace'`, `__proto__='user-supplied-proto'`, `real_field=42` | **site 3**: the rendered UDT. Distinct, recognizable values, so an overwrite is *visible* rather than merely absent. |
| `p` | `frozen<plain>` | `label='no-colliding-field'`, `real_field=7` | the non-colliding contrast — a UDT with no `_type` field at all, where reading `_type` out of the field namespace must fail. |
| `cm` | `map<frozen<collide>,int>` | key `_type='key-type-marker'`, `_keyspace='key-keyspace-marker'`, `__proto__='key-proto-marker'`, `real_field=100` → 1 | the shape a user would naturally write, and the MULTICELL half of the standing parity control: the key lives in the CELL PATH, so it decodes through different code from the frozen `fcm` below. **MEASURED (post-#3612): reaches both bindings' UDT rendering, with key facts identical to `fcm`'s** — see below. |
| `tm` | `map<frozen<collide_twin>,int>` | same field values → 2 | the same, one type over. The four map columns' VALUES (1/2/3/4) are pairwise distinct on purpose: the keys are identical, so only the value tells a reader which column's cell they actually got. |
| `fcm` | `frozen<map<frozen<collide>,int>>` | same field values → 3 | **site 4's actual subject**: decodes to `Frozen(Map([(Udt{…}, 3)]))`, so the key really is a UDT. |
| `ftm` | `frozen<map<frozen<collide_twin>,int>>` | same field values → 4 | same field NAMES and VALUES as `fcm`'s key under a **different type name**: two projected keys that must stay DISTINCT once type identity leaves the field namespace. |
| `fs` | `frozen<set<frozen<collide>>>` | `_type='set-member-type'`, `_keyspace='set-member-keyspace'`, `__proto__='set-member-proto'`, `real_field=200` | the set path into the same projection (`set_to_py` shares `value_to_hashable_key` with map keys). |

`id 3`'s `c` is `_type=NULL`, `_keyspace='keyspace-field-only'`, `__proto__=NULL`,
`real_field=0` —
a **second, distinct failure mode**: under the old code a null `_type` *field*
overwrote the injected type name with `None`, which is not the same defect as the
string case. Assert `.type_name` is still the real UDT type and that the `_type`
FIELD is `None`.

### Why both a non-frozen and a frozen map column

A **non-frozen** `map<frozen<udt>,int>` is multicell, so its key lives in the
CELL PATH; a **frozen** map is a single value cell whose key type resolves
through the on-disk marshal element type / the `UdtRegistry`. The two spellings
took different decode paths, and until #3612 only one of them worked.

**Historically** (and this is why both shapes are committed):
`parse_cell_path_key` matched a closed set of **primitive** cell-path types and
fell back to `Value::Blob` for a frozen UDT, so `cm`/`tm` decoded to
`Map([(Blob(<serialized udt bytes>), int)])` and never reached a `Value::Udt`,
while `fcm`/`ftm`/`fs` did. #3504's projection work therefore had to use the
frozen columns as its subject, and the non-frozen ones documented the gap.

**FIXED by #3612.** The cell-path key site (now
`cqlite-core/src/storage/sstable/reader/parsing/row_decoder/complex_column/cell_path_key.rs`)
delegates to the structural decoder `parse_value_from_raw_bytes`, so `cm`/`tm`
decode to `Map([(Udt{…}, int)])` — the SAME key value the frozen `fcm`/`ftm`
produce, which is what
`cqlite-core/tests/issue_3612_multicell_map_composite_key.rs` asserts against
this fixture's `sstabledump` golden. So all four map columns now exercise the
projection, and the pair `cm`/`fcm` is a standing parity control: a regression
in either decode path makes the two disagree.

## Table 2: `udt_hashable_shapes` — the projection totality boundary (R1-2)

**THIS SECTION HAS BEEN RE-MEASURED TWICE, AND THE HISTORY IS THE POINT.** The
boundary moved under #3504 and again under #3500, in different ways, so anything
here stated as a mechanism decays fast. The column below labelled `now` was
measured against THIS tree; the two earlier columns are kept as record.

When #3504 landed, `value_to_hashable_key` had arms for `List`, `Map`, `Frozen`
and `Udt` only, while `Tuple` and `Set` had **none** and fell through to
`value_to_py`. Making a UDT a hashable `cqlite.Udt` **moved** the boundary without
adding an arm, because what made those fall-through shapes unprojectable was the
UDT being an unhashable `dict`. **#3500 then made both `value_to_hashable_key` and
`contains_udt` TOTAL** — every `Value` variant named, no wildcard arm, pinned by
`#[deny(clippy::wildcard_enum_match_arm)]` — which moved it again. VERIFIED in
`bindings/python/src/value_hashable.rs` on this tree: `Tuple` and `Set` now HAVE
arms (`Value::List(items) | Value::Tuple(items)`, then `Value::Set(items)`), and
`contains_udt` traverses `Value::List | Value::Set | Value::Tuple`. So the
"`Tuple` and `Set` have none" premise is **no longer true** and is recorded above
only as the state #3504 was written against.

This table carries one column per side, each in its **own row** — a `TypeError` is
raised while converting a row, so a row holding both would hide the projectable
cell. That is also why the Python suite reads this table by primary key rather
than scanning it.

| row | column | type | before #3504 | after #3504 | now (`+#3500`, measured on this tree) |
|---|---|---|---|---|---|
| 1 | `stu` | `frozen<set<frozen<tuple<frozen<collide>,int>>>>` | `TypeError: unhashable type: 'dict'` | `frozenset({(Udt, 10)})` | `list[tuple[Udt(collide), 10]]` |
| 1 | `mtu` | `frozen<map<frozen<tuple<frozen<collide>,int>>,int>>` | `TypeError: unhashable type: 'dict'` | `{(Udt, 20): 5}` | `dict{tuple[Udt(collide), 20]: 5}` |
| 2 | `ssu` | `frozen<set<frozen<set<frozen<collide>>>>>` | `TypeError: unhashable type: 'list'` | still raises | `list[list[Udt(collide)]]` — **no longer raises** |
| 3 | `stn` | `frozen<set<frozen<tuple<frozen<unhashable_fields>,int>>>>` | `TypeError: unhashable type: 'dict'` | `frozenset({(Udt, 30)})` | `list[tuple[Udt(unhashable_fields), 30]]` |

Three things to read off it, none of which is what the pre-#3500 text said:

- **NOTHING in this table raises today.** What distinguishes the columns now is the
  CONTAINER, not projectability: `contains_udt` traverses the whole subtree, so
  `set_to_py` sees the UDT under the tuple / under the inner set and takes its
  #804 `list` branch for the whole column. That is why the three `set` columns are
  `list` and only `mtu` — a map KEY, which has no #804 branch to take and must
  project — still goes through `value_to_hashable_key`, now via its real `Tuple`
  ARM rather than a fall-through.
- `ssu`'s change is the one to watch, because the older text asserted the
  opposite. It used to fail for a cause #3504 never touched (the inner set has a
  UDT element, `set_to_py` renders it as a `list` for CLI parity (#804), and a
  `list` is unhashable in the outer set — the error text said `'list'`, not
  `'dict'`). #3500 made the OUTER set take the `list` branch too, so there is no
  longer a set to hash it into.
- `stn`'s **decode gap survives and is still the interesting fact**, though it is
  no longer what makes the column succeed: CQLite decodes a collection field
  inside a frozen UDT as `Value::Blob`, so `m` arrives as `bytes` rather than a
  `dict`. MEASURED on this tree: `m` is
  `b'\x00\x00\x00\x01\x00\x00\x00\x01a\x00\x00\x00\x04\x00\x00\x00\x01'`,
  i.e. the serialized form of the golden's `{"a": 1}`, which is the correct value.
  Orthogonal to both #3504 and #3500, and pinned as characterization by
  `bindings/python/tests/test_issue_3504_udt_field_namespace.py`. `Udt.__hash__`
  does still propagate `TypeError` for a genuinely unhashable field value; no
  decoder path reaches that today, so it is asserted on a hand-built value.

The Node binding has no analogous boundary: it builds a real JS `Set`/`Map`,
which need no hashable projection at all.
