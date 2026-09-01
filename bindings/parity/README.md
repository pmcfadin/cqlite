# 3-way cross-binding parity harness (issue #1455)

The same `SELECT`, run through the **Python binding**, the **Node binding**, and
the **CLI** (`--out json`), must produce **equal canonical JSON**.

Before this harness each binding was validated only against its own oracle
(`sstabledump` JSONL for Python, JSONL for Node, JSONL for the CLI), so two
surfaces could drift apart while every suite stayed green. This is the
surface-layer sibling of the read-path differential
(`cqlite-core/tests/point_vs_full_differential.rs`).

## Layout

| File | Role |
|---|---|
| `fixtures.json` | The fixtures: query, schema, **declared CQL column types**, and an (empty) `known_divergence` key |
| `canonical.py` | Canonical form — Python + CLI adapters |
| `canonical.mjs` | Canonical form — Node adapter (independent twin of `canonical.py`) |
| `canonical-vectors.json` | Differential pin: `vectors` both canonicalizers must reproduce exactly, `rows` for the row-building path (hostile column names), `errors` both must REFUSE, and the `floor` block that stops any of them shrinking. `check_schema`/`checkSchema` require every case to carry every field before any sweep runs |
| `vectors.py` / `vectors.mjs` | Vector runners for each language (`vectors.mjs --emit <path>` writes the JS canonical values THROUGH `JSON.stringify`) |
| `driver.py` / `driver.mjs` | Per-leg runners; write `out/py.<fixture>.json` / `out/node.<fixture>.json` |
| `out/` | Artifacts (gitignored) |
| `../python/tests/test_cross_binding_parity.py` | The comparator (pytest) + negative controls |

## Running it

```bash
export CQLITE_DATASETS_ROOT=/path/to/datasets     # the root fetch-datasets.sh prints
. target/parity-venv/bin/activate                  # a venv with the built `cqlite` wheel
cd bindings/node && npm run build && cd ../..      # the Node leg needs a built native module
cargo build --package cqlite-cli --release         # the CLI leg needs a RELEASE binary

env CQLITE_REQUIRE_FIXTURES=1 CQLITE_PARITY_REQUIRE_NODE=1 RUN_SLOW_TESTS=1 \
  pytest bindings/python/tests/test_cross_binding_parity.py -v
```

Each leg is also runnable standalone:

```bash
python bindings/parity/driver.py            # -> out/py.<fixture>.json
node   bindings/parity/driver.mjs           # -> out/node.<fixture>.json
python bindings/parity/vectors.py           # canonicalizer pin + refusals + floor, python & cli
node   bindings/parity/vectors.mjs          # canonicalizer pin + refusals + floor, node
node   bindings/parity/vectors.mjs --emit out/node.vectors.json   # JSON-boundary artifact
```

Both vector runners print a per-(vector, leg) tally — a vector failing on more
than one leg cannot under-count — plus the refusal tally and the floor verdict.

Both drivers **exit non-zero** when the datasets are present but a query raises
or returns **zero rows**. A 0-row pass over a present corpus is the exact
false-green this repository forbids.

## Canonical form

Canonicalization is **type-driven** from the CQL type declared in
`fixtures.json`, never inferred from a value's runtime shape (issue #28,
no-heuristics). `frozen<X>` is transparent.

| CQL type | Canonical JSON |
|---|---|
| any null / absent column | `null` |
| `boolean` | JSON bool |
| `tinyint` `smallint` `int` `bigint` `counter` `varint` | JSON **number** when `abs(v) <= 2^53-1`, else a decimal **string** |
| `float` `double` | JSON number |
| `text` `ascii` `varchar` | JSON string |
| `blob` | `"0x"` + lowercase hex (`"0x"` when empty) |
| `uuid` `timeuuid` | lowercase hyphenated string |
| `timestamp` | integer **epoch milliseconds** |
| `date` | `"YYYY-MM-DD"` string |
| `time` | integer **nanoseconds since midnight** |
| `duration` | `{"months": int, "days": int, "nanos": int}` |
| `decimal` | plain decimal string; scale trailing zeros PRESERVED, exponent expanded |
| `inet` | string |
| `list<T>` | JSON array, **order preserved** |
| `set<T>` | JSON array, **sorted by canonical element** |
| `map<K,V>` | JSON array of `[k, v]` pairs, **sorted by canonical key** |
| `tuple<...>` | JSON array — see the declared gap below |
| UDT | **refused** — see the declared gap below |

**The integer rule.** One rule, applied identically on all three legs: an
integer JavaScript can represent exactly stays a JSON number; anything outside
`±(2^53-1)` becomes a decimal string. Without it the Node leg (`BigInt`) and the
CLI leg (a JSON number literal) could not be compared at all above 2^53.

**The sort order.** Sets and map entries are ordered by `canonical_compare` /
`canonicalCompare`: rank by type (`null` < bool < number < string < array <
object), then compare within the type; **strings compare by Unicode CODE
POINT**. JS `<` compares UTF-16 code units, which would order astral characters
differently from Python — `canonical.mjs` re-implements code-point order
explicitly, and `set_text_codepoint_order` in `canonical-vectors.json` pins it.

**Two implementations, pinned.** `canonical.py` and `canonical.mjs` are
independent implementations of the table above. They are only *known* to agree
because `canonical-vectors.json` drives both against the same expected output.
Do not change one without re-running `vectors.py` **and** `vectors.mjs`.

## DECLARED GAPS

**All seven** are printed by every `test_three_way_parity` run (from
`DECLARED_GAPS` in the test module), because a lane that omits coverage
silently is indistinguishable from one that covers it. An earlier revision
printed three of the seven under a claim that it printed them all — a false
rationale in a test log is worse than none, because it is what stops the next
person looking. `test_declared_gaps_are_stated_in_full` keeps the count and the
README list in step.

1. **Tuple vs list is undetectable — and on the CLI leg, so is set vs list.**
   The issue asked for `tuple` to
   canonicalize as `{"__tuple__": [...]}` so it could not be confused with a
   list. That tag is **unrecoverable on two of the three legs**: the Node
   binding emits a plain `Array` (`bindings/node/src/value.rs:290`) and the CLI
   emits a plain JSON array (`cqlite-cli/src/output/json.rs:208`); only Python
   has a distinct `tuple`. Tagging Python alone would make every tuple column
   diverge by construction. So a tuple canonicalizes to a **plain array on all
   three legs**, and **this harness cannot detect a tuple/list confusion.**
   The same limit is why F4's type-specific container check (see below) is
   enforced on the python and node legs only: `cqlite-cli`'s JSON writer
   renders `list`, `set` and `tuple` all as a bare array, so that leg cannot
   distinguish any of them.
2. **No `varint` fixture exists.** No committed schema under `test-data/schemas/`
   declares a `varint` column (the only occurrence of the word is a comment in
   `cql-type-parity.cql`). The issue's requested
   timestamp+blob+varint+decimal fixture is therefore covered for timestamp,
   blob and decimal only; the varint rule is pinned by `canonical-vectors.json`
   alone. No fixture was fabricated to close this.
3. **UDT columns are refused, not canonicalized.** A UDT needs its declared
   field types, and inferring them from the value would be the heuristic issue
   #28 forbids. `parse_type` raises on a UDT name; no fixture uses one.
4. **Non-finite floats are a real 3-way asymmetry and are avoided.** Python
   yields `nan`/`inf`, Node yields `NaN`/`Infinity`, and the CLI emits `null`
   (`cqlite-cli/src/output/json.rs:156-161`, `serde_json::Number::from_f64`
   returns `None`). No fixture contains one; a future one would need a rule
   agreed across all three writers first.
5. **Absent vs null columns.** Measured, the leg that omits is the **Node**
   one: `bindings/node/src/row.rs:123-138` SKIPS a metadata column with no
   matching value (deliberately — null-filling would emit a phantom `col_0:
   null` for an aggregate). The **Python** binding null-**fills** a shared row
   shape (`bindings/python/src/result.rs:184-192,447`) and the CLI always emits
   every column. The comparator compares the **union** of column names and
   treats an absent key as `null`, so absence is not a difference — but a
   genuinely wrong value still fails, because only the missing side is
   defaulted. `assert_leg_columns` therefore asserts a **subset** relation per
   leg (a leg may omit a declared column, never invent one) and
   `assert_union_columns` asserts the union covers the declared set — the same
   rule the comparator uses, rather than two rules that contradict each other.
   Both drivers compute `observed_columns` as a **union over every row**, never
   the last row's keys.
6. **A uniform defect is invisible.** All three legs read through
   `cqlite-core`. Agreement here is agreement *about CQLite*, not about
   Cassandra. The `sstabledump` JSONL goldens and
   `test-data/query-semantics-oracle.json` remain the correctness authority.
7. **Gate vs CI coverage split.** The comparator is marked `slow`: the local
   agent gate runs pytest with `RUN_SLOW_TESTS=0` and builds neither a release
   `cqlite-cli` nor the Node native module, so **the 3-way comparison runs in
   CI only** (`.github/workflows/python-ci.yml`, job `cross-binding-parity`).
   What DOES run in the gate is `test_canonicalizer_vectors_python_leg` — pure
   computation, no corpus, no build. `.github/ci-gating-tiers.yml` records this
   residual on the `python-ci.yml` exemption.

## `known_divergence` is empty, and must stay that way

Every fixture carries `"known_divergence": []`. The key exists so a future,
deliberately-accepted gap has somewhere to live **with a reason attached**; it
is not a mute button. `test_fixtures_declare_no_divergence_allowlist` fails if
anything is added.

The issue originally proposed allowlisting `duration` and `time` "until #1450
lands". **#1450 is landed**, and all three legs were measured to agree exactly
on both (`duration_val` = `{0, 0, 46702000000000}`, `work_time` =
`4325394017000`), so nothing is allowlisted. **A real divergence is a bug to
report, not an entry to add.**

## Containers are TYPE-SPECIFIC, per leg

A `list`, a `set` and a `tuple` are **distinguishable public API shapes**, and
a binding regression that returns an `Array` for a declared `set<text>` (or a
`Set` for a `list<int>`) is exactly the cross-binding drift this harness
exists to catch. The generic sequence adapters used to accept all three
interchangeably and normalize the difference away, so the regression PASSED.

| declared kind | python leg | node leg | cli leg |
|---|---|---|---|
| `list<T>` | `list` | `Array` | JSON array |
| `set<T>` | `frozenset` / `set` | `Set` | JSON array |
| `tuple<...>` | `tuple` | `Array` (declared gap 1) | JSON array |
| `map<K,V>` | `dict` | `Map` | array of `{key,value}` |

A mismatch is refused with a message naming the declared kind, the expected
container and what actually arrived (`declared set<> expects a JavaScript Set,
got Array`).

**The CLI leg cannot participate**, and that is declared rather than papered
over: `cqlite-cli/src/output/json.rs` renders list, set and tuple all as a bare
JSON array, so on that leg the check is only "is it an array". The enforcement
is on the python and node legs.

### Scalars: the same rule, one level down — and NODE-ONLY

The Node adapter used to accept `number` **or** `bigint` for every integer
kind, so a regression returning a `number` where the surface is `BigInt` passed
for every value below 2^53 and would only ever have surfaced past it: silent
for every realistic fixture, which is the worst failure mode. The exact type is
now required per kind, verified at `bindings/node/src/value.rs`:

| declared kind | node leg | source |
|---|---|---|
| `tinyint` `smallint` `int` | `number` | `create_int32` (`:214-216`) |
| `bigint` `counter` | `BigInt` | `create_bigint_from_i64` (`:219-220`) |
| `time` | `BigInt` | `create_bigint_from_i64` (`:249`) |
| `varint` | `BigInt` | `varint_to_bigint` (`:259`) |
| `duration.months` `duration.days` | `number` | `create_int32` (`:337-338`) |
| `duration.nanos` | **`BigInt`** | `create_bigint_from_i64` (`:339-340`) |

The **nested** `duration.nanos` is the one easiest to miss — `months` and
`days` beside it are plain numbers — and it has its own refusal case.

**This is node-only**, the same asymmetry the container table has one row up,
and for a matching reason: the **Python** binding returns a plain `int` for
every integer kind, so there is nothing to distinguish; the **CLI** emits a
JSON number for all of them (a decimal string for `varint`), so nothing there
either. The enforcement lives where the surface is unambiguous, which is the
only place a strictness rule can be added without risking a false red.

### A PRESENT `undefined` is a regression, not an absence

Declared gap 5 accommodates an **absent** property — the Node binding
legitimately omits a metadata column with no value. `canonRowNode` decides
absence with `hasOwnProperty` (never with an `=== undefined` test, or the
distinction could not be made at all) and supplies `null` for it. A property
that IS present and holds `undefined` is a binding regression: the binding
cannot produce it, and `JSON.stringify` would drop it from the artifact
silently. It is refused, and the rule reaches container **elements** too, so a
sparse array's hole is refused as well.

### Three intentional projections, each measured at the binding's source

1. **`SET<FROZEN<UDT>>` is a Python `list`** (#804/#3500).
   `bindings/python/src/value.rs::set_to_py` branches on
   `items.iter().any(contains_udt)` — **UDT containment, not unhashability**.
   Currently unreachable here (`parse_type` refuses UDT names), so the
   allowance is implemented and tested (`test_set_of_udt_projection_is_allowed`
   builds the type tree directly) purely so that adding UDT support later
   cannot turn a correct binding red.
2. **Hashable positions project every container.** Inside a `set` element or a
   `map` KEY, `bindings/python/src/value_hashable.rs::value_to_hashable_key`
   turns `list`/`tuple` into a Python `tuple`, keeps `set` as a `frozenset`,
   and turns `map` into a `tuple` of 2-`tuple`s. So a `set<frozen<list<int>>>`
   is a `frozenset` **of tuples**, and a context-free strict check would red on
   correct input. `_canon` therefore carries a `hashable` flag, set when
   descending into a set element or a map key and never cleared (that function
   recurses into itself). A map **value** is *not* a hashable position
   (`map_to_py` projects only the key), and that asymmetry has its own vector.
   *This projection was not anticipated when the fix was requested; it was
   found by the check reddening a vector of ours that was itself wrong about
   the binding, and the vector was corrected against the source.*
3. **Node cannot distinguish `tuple` from `list`** — both are `Array`
   (`bindings/node/src/value.rs:290`). Kept a **declared gap**, not turned into
   a refusal. The Node binding has no hashable projection at all: measured,
   `set_to_js_set` / `map_to_js_map` / `list_to_array` recurse through
   `value_to_napi` unconditionally, which is why only the Python adapter
   carries the flag.

Every accepted shape and every refused shape has a vector
(`set_of_frozen_set`, `map_key_frozen_list`, `map_key_frozen_map`,
`map_value_frozen_list_is_not_projected`, `tuple_containing_a_list`,
`list_of_frozen_set`; refusals `python_list_for_set`, `python_tuple_for_list`,
`node_array_for_set`, `node_set_for_list`, `node_map_for_set`,
`node_object_for_map`, …). Verified by planting: swapping a `Set` for an
`Array` in a node vector reds `vectors.mjs` naming the kind; the mirror swap on
the python leg reds `vectors.py`.

## Presence is required — no permissive defaults

A `.get(key, default)` in Python or a `|| []` / `?? 0` in JS reads a
**three-valued** signal (present-with-a-value / present-and-null / **missing**)
**two-valued**, and always picks the permissive branch. Accidentally delete a
vector's `cli` key and that leg was silently skipped — the differential pin
stayed green over a shrunken subject set. The case floors count CASES; they
could not see an incomplete case.

Both runners now index **directly** everywhere, and `check_schema` /
`checkSchema` (twins over the same file, run BEFORE any sweep) turn the
resulting error into a named message: every top-level section present; every
`floor` key present; every vector carrying `name`/`type`/`canonical` **and all
three leg keys**; every row case the same plus `columns`; every error case
`name`/`stage`/`expect`/`type`, with a **non-empty** `legs` when
`stage=canonicalize` (an empty one would verify nothing and still count).
`fixtures.json` gets the same treatment in both drivers.

**A leg is skipped only by an EXPLICIT `null`**; an absent key is a named
refusal. Counts are reported **affirmatively** — `0 RECOGNISED leg-skips`,
never a bare `0` — because a bare zero in a run log reads as a verified
all-clear from a scan that may never have run.

## Case floors — the subject set cannot shrink to nothing

#3544's own lesson applied to this harness. Every subject set here could
previously go to zero and stay green: an empty `fixtures.json` yields an EMPTY
pytest parametrize (one skipped placeholder, no 3-way comparison); an empty
`canonical-vectors.json` makes both runners print `0/0 vectors OK` and exit 0.

Both tables now carry a committed `floor` block that **both** runners enforce
before reporting:

* `fixtures.json` → `min_fixtures` **and** `required_names` (a count alone
  would let a fixture be swapped for a trivial substitute).
* `canonical-vectors.json` → `min_vectors`, `min_errors`, `min_rows`,
  `required_row_names`, `required_error_names` (the strictness refusals named
  rather than counted, since each pins a defect that is silent when it is not
  refused), `required_kinds` (checked against the CQL kinds
  appearing anywhere in each vector's parsed type tree, so deleting every blob
  vector reds the runners), `require_nested_container` and
  `require_null_canonical`. `required_row_names` is named rather than counted
  because the `__proto__` cases are the reason `rows` exists and a count alone
  would let them be swapped for benign rows.

Verified by planting the break: emptying either table, deleting the `floor`
block, or removing every blob vector reds **both** `vectors.py` and
`vectors.mjs`.

## Refusals are pinned too

`canonical-vectors.json`'s `errors` array holds malformed inputs that each
canonicalizer must **raise** on, with the message naming the reason: a UDT or
unknown scalar type, an unbalanced `<`, a wrong container arity, a tuple arity
mismatch, a non-hyphenated UUID, each unparseable CLI temporal/duration form, a
blob without its `0x` prefix or with non-hex digits, a malformed CLI map entry,
and a non-decimal decimal string. A canonicalizer that guesses at malformed
input is the heuristic issue #28 forbids, so "it refused" is a pinned property.

## Hostile column NAMES (`__proto__`)

`__proto__` is a **legal CQL column name** — expressible as the quoted
identifier `"__proto__"`, and this repository already ships a fixture schema
for it (`test-data/schemas/issue-3630-row-collision.cql`). On an ordinary
JavaScript object, `obj["__proto__"] = v` runs the inherited **setter** on
`Object.prototype` and replaces the prototype instead of creating an own
property. It throws nothing: the column simply disappears from
`Object.keys()` and from the emitted JSON — so the harness would have reported
agreement about a column the Node leg had silently dropped, which is exactly
the class of defect it exists to catch. The Node binding itself already
defends this way (`bindings/node/src/value.rs` uses `Object.create(null)` for
UDT fields and JSON objects); the harness now follows suit.

Every column-name-keyed object in the JS half is therefore built with
`Object.create(null)`: `typesFromColumns` (canonical.mjs, the one builder that
`driver.mjs`'s `fixtureTypes` delegates to), `canonRowNode`'s output, and
`materializeNodeRow` in the vector runner. Python is immune (a `dict` has no
prototype), and `types_from_columns` exists there so both halves share one
entry point into the row path.

The pin is the **`rows` section of `canonical-vectors.json`**: whole-row cases
with `columns` (name → CQL type) plus a per-leg row, driven by BOTH runners
through the real row-building path. Four cases: an ordinary control, a row with
`__proto__` / `constructor` / `toString` / `valueOf` / `prototype` columns, and
the two null shapes (`__proto__` absent from the row, and explicitly null —
`out["__proto__"] = null` also sets the prototype, so it vanishes just the
same). The check is not only value equality: a canonical row that has LOST a
declared column is reported by name.

**No live 3-way fixture exists for this**: neither corpus root holds a
`test_row_collision` SSTable, so there is nothing to `SELECT`. The row cases
plus `test_canonicalizer_row_cases_python_leg`, the JS `checkRows`, and the
JSON round-trip check below are the coverage — stated here rather than left to
be assumed. Verified by planting: restoring the plain-object form reds three of
the four row cases in `vectors.mjs`, `test_canonicalizer_vectors_node_leg` and
`test_node_canonical_survives_the_json_round_trip`, each naming the lost column.

## The JSON boundary

`vectors.mjs`'s in-memory check cannot see serialization:
`JSON.stringify({h: 1.0})` emits `{"h":1}`, and `json.load` hands Python an
`int` where the python and cli legs hold a `float`. That would red this lane on
correct input the first time the corpus grows an integral `double`. Two things
close it: `shape_tag`/`shapeTag` collapse `int` and `float` to one `"number"`
tag (JSON has one number type; `bool` is still checked first, so the rule the
tag enforces — number vs string vs bool vs null — is intact), and
`test_node_canonical_survives_the_json_round_trip` writes every vector AND
every row case's JS canonical value through `JSON.stringify`, re-reads it in
Python and compares — and, for row cases, asserts every declared column is
still an OWN key, so a `__proto__` column that survived in memory but not
through serialization is caught too. Verified by planting the break:
re-splitting `int`/`float` reds both that test and
`test_comparator_accepts_an_integral_float_across_the_json_boundary`.

## Fail-closed, never a silent skip

* **Datasets** — `skip_if_no_datasets()` from `conftest.py`: a FAILURE under
  `CQLITE_REQUIRE_FIXTURES=1`.
* **Schemas** — committed source, asserted **unconditionally** (#3148).
  `conftest.skip_if_no_schema` is a plain skip, so a typo'd `schema` path in
  `fixtures.json` would otherwise drop a fixture even under strict mode.
* **The CLI binary** — `resolve_parity_cli_binary` wraps `conftest.cli_binary`,
  whose four skip routes (build failure, timeout, no cargo, missing binary) are
  NOT strict-aware. In the CI job the invocation is this file alone, whose
  non-slow tests pass, so #1230's "no tests ran" session floor never fires and
  all three parity cases would skip while the job reported success having
  compared nothing. Under either strict switch that skip becomes a failure.
  `conftest.py` is deliberately not edited — other suites rely on the lenient
  skip.
* **The Node leg** — `CQLITE_PARITY_REQUIRE_NODE=1` (its OWN switch;
  `CQLITE_REQUIRE_FIXTURES` is about the dataset corpus and the existing
  python-ci `test` job sets it while never provisioning Node, so binding the
  two would red that job on correct input). Without the switch and without the
  artifact, the run compares python vs cli and **declares the omitted leg** in
  its output. `node_leg_disposition()` is a pure function so both branches are
  unit-tested rather than assumed.

## Row order

Measured on this corpus: all three legs return rows in the **same** order
(verified over 500 rows of `test_basic.simple_table` as well as every fixture),
so the comparator compares rows **in order** and never sorts them. An order
difference is a real finding.
