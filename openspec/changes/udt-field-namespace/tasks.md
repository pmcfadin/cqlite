# Tasks: out-of-band UDT type identity (#3504)

Public surface exercised by each task is named, per `openspec/config.yaml`.

## 1. Python production surface
- [ ] 1.1 Add `#[pyclass(module = "cqlite", frozen)] Udt` in `bindings/python/src/value.rs` with
  `#[pyo3(get)] type_name`, `keyspace`, `fields`, following the `cqlite.Duration` precedent.
  **Surface**: `cqlite.Udt`.
- [ ] 1.2 Implement `__getitem__`/`__contains__`/`__iter__`/`__len__`/`keys`/`values`/`items`/`__repr__`
  delegating to `fields`, plus `__eq__`/`__hash__` over `(keyspace, type_name, fields)`.
  **Surface**: mapping access `udt["street"]`; hashability required by task 3.
- [ ] 1.3 Rewrite `udt_to_py` to construct `Udt` with fields ONLY — no `_type`/`_keyspace` set_item.
  **Surface**: every Python query result containing a UDT.
- [ ] 1.4 Export `Udt` from the module and declare it in `python/cqlite/__init__.pyi`.
  **Surface**: `from cqlite import Udt`; asserted by `test_stub_fidelity.py`.

## 2. Node production surface
- [ ] 2.1 Rewrite `udt_to_object` in `bindings/node/src/value.rs` to emit
  `{ typeName, keyspace, fields }` with fields in their own nested object.
  **Surface**: every Node query result containing a UDT.
- [ ] 2.2 Update `interface UdtValue` in `bindings/node/lib/index.d.ts` — remove the
  `[field: string]: Value` index signature, add `typeName`/`keyspace`/`fields`; update the JSDoc
  example and the type-mapping table row. **Surface**: `index.d.ts`; asserted by
  `typescript-definitions.test.js`.

## 3. Site 4 — the Python hashable projection
- [ ] 3.1 Rewrite the `Udt` arm of `value_to_hashable_key` to emit a `Udt` instance: metadata on the
  instance, exactly one pair per declared field, no metadata pairs.
  **Surface**: `dict` keys from `map<frozen<udt>,X>` (`map_to_py`) and set members (`set_to_py`).
- [ ] 3.2 Do NOT add `Tuple`/`Set` arms (#3500). Confirm no behaviour change for shapes that
  currently succeed.

## 4. Tests (each maps to a spec scenario)
- [ ] 4.1 Rust unit tests in `bindings/python/src/value.rs` constructing a `Value::Udt` with fields
  named `_type` and `_keyspace`: assert type identity + all three field values recoverable, and
  `len == 3`. **Executed by**: `binding-rust-tests`.
- [ ] 4.2 Rust unit tests in `bindings/node/src/value_tests.rs` for the same input asserting the
  `{typeName, keyspace, fields}` shape. **Executed by**: `binding-rust-tests`.
- [ ] 4.3 Site-4 unit test: colliding-field UDT as a map key → exactly one `_type` entry; plus the
  two-different-types-same-fields distinctness case. **Executed by**: `binding-rust-tests`.
- [ ] 4.4 Python-level tests: update `test_types_collections_udt.py` (6 assertion sites) from
  `"_type" in udt` to `.type_name`; keep them dataset-skip-clean.
  **Executed by**: `python-bindings`.
- [ ] 4.5 Node-level tests: update `types.test.js` (5 sites) to the new shape.
  **Executed by**: `node-bindings`.
- [ ] 4.6 `test_cli_parity.py`: retarget `test_udt_field_named_keyspace_is_dropped` (it pins the
  DEFECT — rewrite to pin the fix), update the a-3 projection expectation and `_udt()` helper, and
  retype the normalizer's UDT branch off the `"_type" in value` sniff. Leave the site-2 test
  (`test_map_with_literal_type_key_is_misclassified_as_a_udt`) and the site-1 test asserting their
  current, still-true behaviour. **Executed by**: `python-bindings`.
- [ ] 4.7 Stub-fidelity: `test_stub_fidelity.py` green with `TYPE_ONLY_STUB_NAMES` still empty;
  `typescript-definitions.test.js` drift alarm + no-`any` green.
- [ ] 4.8 Cross-binding parity assertion for AC3 (same input → equal facts both sides).

## 5. Docs
- [ ] 5.1 `docs/development/M4_spec.md` §5.3: sites 3+4 → FIXED with mechanism; site 2 stays OPEN
  attributed to #3497, noting the new structural signal; correct the oracle table.
- [ ] 5.2 `bindings/python/README.md` (:308) and `bindings/node/README.md` (:352), the module doc
  tables (`python/src/value.rs`, `node/src/value.rs`), and `node/examples/type-handling.ts` +
  `error-handling.ts` — no example may still show the flat shape as current.
- [ ] 5.3 CHANGELOG: note the breaking binding-surface change and the migration
  (`udt["_type"]` → `udt.type_name` / `result._type` → `result.typeName`).

## 6. Certification
- [ ] 6.1 `scripts/agent-gate.sh --lite` PASS each fix round (summary-file redirect).
- [ ] 6.2 `rust-reviewer` + sanctioned roborev clean on the lite-green diff, BEFORE any full gate.
- [ ] 6.3 ONE full `scripts/agent-gate.sh` (gate of record) in `flow-closer`; `spec-auditor` C PASS
  against this spec; final roborev clean; then `gh pr merge --auto --squash --delete-branch`.
