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
| `canonical-vectors.json` | Differential pin: 59 vectors both canonicalizers must reproduce exactly |
| `vectors.py` / `vectors.mjs` | Vector runners for each language |
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
python bindings/parity/vectors.py           # canonicalizer pin, python + cli
node   bindings/parity/vectors.mjs          # canonicalizer pin, node
```

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

These are stated at run time by the test itself, because a lane that omits
coverage silently is indistinguishable from one that covers it.

1. **Tuple vs list is undetectable.** The issue asked for `tuple` to
   canonicalize as `{"__tuple__": [...]}` so it could not be confused with a
   list. That tag is **unrecoverable on two of the three legs**: the Node
   binding emits a plain `Array` (`bindings/node/src/value.rs:290`) and the CLI
   emits a plain JSON array (`cqlite-cli/src/output/json.rs:208`); only Python
   has a distinct `tuple`. Tagging Python alone would make every tuple column
   diverge by construction. So a tuple canonicalizes to a **plain array on all
   three legs**, and **this harness cannot detect a tuple/list confusion.**
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
5. **Absent vs null columns.** The Python binding omits null columns while the
   CLI always emits them. The comparator compares the **union** of column names
   and treats an absent key as `null`, so absence is not a difference — but a
   genuinely wrong value still fails, because only the missing side is
   defaulted.
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

## Row order

Measured on this corpus: all three legs return rows in the **same** order
(verified over 500 rows of `test_basic.simple_table` as well as every fixture),
so the comparator compares rows **in order** and never sorts them. An order
difference is a real finding.
