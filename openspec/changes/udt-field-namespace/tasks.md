# Tasks: out-of-band UDT type identity (#3504)

Public surface exercised by each task is named, per `openspec/config.yaml`.

## 1. Python production surface
- [x] 1.1 Add `#[pyclass(module = "cqlite", frozen)] Udt` in `bindings/python/src/value.rs` with
  `#[pyo3(get)] type_name`, `keyspace`, `fields`, following the `cqlite.Duration` precedent.
  **Surface**: `cqlite.Udt`.
- [x] 1.2 Implement `__getitem__`/`__contains__`/`__iter__`/`__len__`/`keys`/`values`/`items`/`__repr__`
  delegating to `fields`, plus `__eq__`/`__hash__` over `(keyspace, type_name, fields)`.
  **Surface**: mapping access `udt["street"]`; hashability required by task 3.
- [x] 1.3 Rewrite `udt_to_py` to construct `Udt` with fields ONLY — no `_type`/`_keyspace` set_item.
  **Surface**: every Python query result containing a UDT.
- [x] 1.4 Export `Udt` from the module and declare it in `python/cqlite/__init__.pyi`.
  **Surface**: `from cqlite import Udt`; asserted by `test_stub_fidelity.py`.

## 2. Node production surface
- [x] 2.1 Rewrite `udt_to_object` in `bindings/node/src/value.rs` to emit
  `{ typeName, keyspace, fields }` with fields in their own nested object.
  **Surface**: every Node query result containing a UDT.
- [x] 2.1a Build `fields` with a NULL PROTOTYPE (`Object.create(null)`, the handle cached on
  `ConvCtx` beside the `Set`/`Map` constructors) — roborev R1-1. A nested object is not enough while
  it inherits accessors: a field named `__proto__` reached `Object.prototype`'s inherited setter, so
  a string value VANISHED and a null value REPLACED the prototype (both measured on the fixture,
  which now declares such a field). Deliberately NOT a special case on the literal name.
  **Surface**: `udt.fields` on every Node query result; asserted by three cases in
  `bindings/node/__test__/issue-3504-udt-field-namespace.test.js`.
- [x] 2.2 Update `interface UdtValue` in `bindings/node/lib/index.d.ts` — remove the
  `[field: string]: Value` index signature, add `typeName`/`keyspace`/`fields`; update the JSDoc
  example and the type-mapping table row. **Surface**: `index.d.ts`; asserted by
  `typescript-definitions.test.js`.

## 3. Site 4 — the Python hashable projection
- [x] 3.1 Rewrite the `Udt` arm of `value_to_hashable_key` to emit a `Udt` instance: metadata on the
  instance, exactly one pair per declared field, no metadata pairs.
  **Surface**: `dict` keys from `map<frozen<udt>,X>` (`map_to_py`) and set members (`set_to_py`).
- [x] 3.2 Do NOT add `Tuple`/`Set` arms (#3500) — and MEASURE what making `Udt` hashable does to
  totality rather than assuming it does nothing. Measured (roborev R1-2, fixture table
  `udt_hashable_shapes`, `origin/main` built into the same venv for the "before" column): a UDT
  reached through the arm-less `Tuple` fallthrough in a hashed position now PROJECTS
  (`set<frozen<tuple<frozen<udt>,int>>>` and `map<frozen<tuple<frozen<udt>,int>>,int>`, both
  previously `TypeError: unhashable type: 'dict'`), while a UDT-bearing `set` in a hashed position
  still raises `TypeError: unhashable type: 'list'` — unchanged, and for the #804 list-rendering
  reason, not this one. The earlier claim "#3500 neither fixed nor worsened" was FALSE and is
  corrected in the spec scenario and in `design.md`.

## 4. The test subject — a Cassandra-written colliding UDT fixture

No corpus fixture declares a `_type`/`_keyspace`/`__proto__` field, and the issue names generating one
as part of the fix. **Cassandra-written, not CQLite-written**, and committed **checkout-relative**.

- [x] 4.1 `test-data/scripts/generate-issue-3504-udt-collision.sh` on the
  `generate-compaction-parity-udt.sh` pattern: Cassandra 5.0 container,
  `CREATE TYPE collide ("_type" text, "_keyspace" text, "__proto__" text, real_field int)` (quoted
  identifiers — `parse_create_type` already accepts them, and Cassandra 5.0.2 accepts all three
  names; `"__proto__"` was added for roborev R1-1 and `collide_twin` carries it too, because the
  same-fields/different-type distinctness test asserts equal field mappings as its precondition),
  one table with a `frozen<collide>` column AND a `map<frozen<collide>, int>` column (the latter is
  site 4's subject), insert, `nodetool flush`, export.
- [x] 4.1a A SECOND table `udt_hashable_shapes` carrying one column per side of the
  hashable-projection totality boundary (roborev R1-2), each in its own row because the `TypeError`
  is raised while converting a row. See task 3.2 for the measurement it produced.
- [x] 4.2 Commit the SSTable components with `git add -f` (`*.db` is gitignored; force-adding tiny
  parity references is mandated doctrine) under a **checkout-relative per-issue** directory on the
  `cqlite-core/tests/fixtures/issue_2225/` precedent — NOT under `test-data/datasets/sstables/`.
  **Why:** `bindings/python/tests/conftest.py` and `bindings/node/__test__/setup.js` resolve the
  corpus as an EITHER/OR on `CQLITE_DATASETS_ROOT` — unset, they do use the checkout; **set, which
  every gate run does, the checkout copy is never consulted** — so a corpus-rooted fixture is
  invisible on any box with that env set, which is every gate run. A checkout-relative path
  cannot be hidden by an env var. Commit the `.cql` schema alongside.
- [x] 4.3 Record the sstabledump JSONL golden for the new table.

## 5. Tests (each maps to a spec scenario; executor named because several candidate homes execute NOTHING)

**Constraint that shapes all of this**: `cargo test -p cqlite-py` is *structurally impossible* (a pyo3
`cdylib` test harness cannot link libpython — no `auto-initialize`), so a `#[cfg(test)]` test in
`bindings/python/src/**` executes in **no component anywhere** and is a dead test. And a napi `Env`
cannot be fabricated off-thread, so `udt_to_object`'s produced object cannot be asserted from
`bindings/node/src/value_tests.rs`. Therefore the collision assertions live at script level.

- [x] 5.1 Python, `bindings/python/tests/test_issue_3504_udt_field_namespace.py`: read the 4.2 fixture
  through the public query API; assert `.type_name`/`.keyspace` plus every field value, the exact
  field-NAME SET (a count cannot see a lost field — R1-1), `udt["_type"]` returns the FIELD, and
  `udt["_type"]` on a non-colliding UDT raises `KeyError`.
  **Executed by**: `python-bindings` (`maturin develop` + `pytest bindings/python/tests`).
- [x] 5.1a Python, same file: the R1-2 boundary — the two shapes that now project (asserted by
  RETRIEVING each projected key with an independently constructed equal value, not merely by the
  absence of an exception), the one that still raises (matched on `unhashable type: 'list'`, the text
  that identifies the #804 list rendering as the cause), the whole-table scan that the failing row
  aborts, and the `stn` decode-gap characterization plus the `Udt.__hash__` residual.
  **Executed by**: `python-bindings`.
- [x] 5.2 Node, `bindings/node/__test__/issue-3504-udt-field-namespace.test.js`: same input, asserting
  `{typeName, keyspace, fields}` and that `Object.keys(result)` holds no field name.
  **Executed by**: `node-bindings` (whole jest suite).
- [x] 5.2a Node, same file: the R1-1 `__proto__` cases — own enumerable DATA property with the
  declared value (descriptor asserted, since only the descriptor distinguishes "defined the field"
  from "wrote through a setter"), the null-valued field, and a null prototype across seven field bags
  including key and element position. Expectations are built with `Object.fromEntries`, because an
  object LITERAL cannot express `__proto__` as a property at all. Verified non-vacuous: 8 of the 13
  cases fail against the pre-fix binary. **Executed by**: `node-bindings`.
- [x] 5.3 Site 4 (Python only): the `map<frozen<collide>, int>` column → exactly one `_type` entry in
  the projected key, identity recoverable; plus two-different-types-same-fields distinctness.
  **Executed by**: `python-bindings`.
- [x] 5.4 Cross-binding parity for AC3: same fixture, compare type name / keyspace / field mapping as
  DATA across the two suites.
- [x] 5.5 Update `test_types_collections_udt.py` (6 assertion sites) from `"_type" in udt` to
  `.type_name`; keep dataset-skip-clean. **Executed by**: `python-bindings`.
- [x] 5.6 Update `types.test.js` (5 sites) to the new shape. **Executed by**: `node-bindings`.
- [x] 5.7 `test_cli_parity.py`: retarget `test_udt_field_named_keyspace_is_dropped` — it pins the
  DEFECT, so rewrite it to pin the fix; update the a-3 projection expectation and the `_udt()` helper;
  retype the normalizer's UDT branch off the `"_type" in value` sniff (production no longer emits that
  key, so leaving the sniff makes it dead for UDTs while still firing on maps). Leave the site-1 and
  site-2 tests asserting their current, still-true behaviour. **Executed by**: `python-bindings`
  (CLI-parity suite needs `RUN_SLOW_TESTS=1`).
- [x] 5.8 Stub fidelity: `test_stub_fidelity.py` green with `TYPE_ONLY_STUB_NAMES` still **empty**;
  `typescript-definitions.test.js` drift alarm + the no-`any` rule green.
- [ ] 5.9 **State the SKIP exposure in the PR rather than papering over it**: both `python-bindings`
  and `node-bindings` SKIP without their toolchain, so AC2's evidence rides on SKIP-able components.
  Do NOT manufacture a never-SKIP `binding-rust-tests` test by inventing an Env-free abstraction the
  fix does not otherwise need — a harness that never reaches the code is worse than a declared SKIP.

## 6. Docs
- [x] 6.1 `docs/development/M4_spec.md` §5.3: sites 3+4 → FIXED with mechanism; site 2 stays OPEN
  attributed to #3497, noting the new structural signal; correct the oracle table.
- [x] 6.2 `bindings/python/README.md` (:308) and `bindings/node/README.md` (:352), the module doc
  tables (`python/src/value.rs`, `node/src/value.rs`), and `node/examples/type-handling.ts` +
  `error-handling.ts` — no example may still show the flat shape as current.
- [x] 6.3 CHANGELOG: note the breaking binding-surface change and the migration
  (`udt["_type"]` → `udt.type_name` / `result._type` → `result.typeName`).

## 7. Certification
- [ ] 7.1 `scripts/agent-gate.sh --lite` PASS each fix round (summary-file redirect).
- [ ] 7.2 `rust-reviewer` + sanctioned roborev clean on the lite-green diff, BEFORE any full gate.
- [ ] 7.3 ONE full `scripts/agent-gate.sh` (gate of record) in `flow-closer`; `spec-auditor` C PASS
  against this spec; final roborev clean; then `gh pr merge --auto --squash --delete-branch`.
