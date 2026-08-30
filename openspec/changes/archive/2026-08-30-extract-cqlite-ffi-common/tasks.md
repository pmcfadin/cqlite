# Tasks: extract-cqlite-ffi-common (issue #1452)

> Design decided in `design.md`. In one line: create the pure workspace member `cqlite-ffi-common`,
> make it the single implementation of DECIMAL / VARINT / INET rendering and the home of the #1451 FFI
> error contract and the OTel key list, rewire both bindings to thin adapters, and pin the
> "single implementation" property with a committed vector table both binding suites assert.
> Issue-step → requirement map is at the top of `specs/binding-shared-scalar-math/spec.md`.
>
> **THREE OWNER DECISIONS GATE PARTS OF THIS LIST — do not guess them.** OQ1 (the #1705
> `ErrorCategory` rename) gates task 5b. OQ2 (`error_schema::classify()` as the table's authority)
> gates nothing here if the answer is "no/later" — if it is "yes", STOP and re-scope, because the
> table's `category` column changes value and that is a different change. OQ3 (the single DECIMAL
> policy) gates tasks 3a/3c/9. They are stated with options, a recommendation and a safe default in
> the Seam-1 approval comment on issue #1452.

## 1. Crate skeleton (surface: root `Cargo.toml`, `cqlite-ffi-common/Cargo.toml`)
- [x] Create `cqlite-ffi-common/` with `Cargo.toml`: workspace-inherited `version`/`edition`/`authors`/
      `license`/`repository`/`rust-version`, `publish = false`, `[lints] workspace = true`.
- [x] Dependencies: `cqlite-core = { path = "../cqlite-core" }` and `num-bigint = "0.4"` (declared
      locally to match `cqlite-core/Cargo.toml:80`; **do not** create a `[workspace.dependencies]`
      entry — non-goal).
- [x] Add `"cqlite-ffi-common",` to the root `Cargo.toml` `members` list. The two binding crates are
      **already** members (`Cargo.toml:2-13`) — no membership surgery there.
- [x] `src/lib.rs`: crate docs stating the dependency direction, the no-FFI-framework rule and its
      reason, and the deliberate OTel *mechanism* divergence (Python rejects unknown keys, napi drops
      them). Re-export the module items at the crate root only where it removes a stutter; keep module
      paths otherwise.
- [x] Commit.

## 2. Dependency-boundary test FIRST, RED before GREEN (surface: `cqlite-ffi-common/tests/dependency_boundary.rs`)
- [x] Implement the `cargo metadata` closure measurement of `design.md` D6: resolve the graph rooted at
      `cqlite-ffi-common`, assert no package name is/starts with `pyo3`, `napi`, `napi-derive`, and
      assert the closure is non-empty and contains `cqlite-core`.
- [x] Fail-closed on: `CARGO` unset, non-zero exit, unparseable JSON, missing own node. Each FAILs with
      a message naming what could not be measured. **No env opt-out.**
- [x] **RED-verify the assert before believing it**: in a scratch copy of the crate, add a dependency
      that pulls `pyo3` and confirm the test FAILs; then break `cargo metadata` (e.g. point `CARGO` at
      `/bin/false`) and confirm it FAILs rather than passing. Record both in the PR body.
- [x] Commit.

## 3. DECIMAL (surface: `cqlite-ffi-common/src/decimal.rs`)
- [x] **3a. Apply the OQ3 ruling.** Implement the single policy the owner chose, as named `pub const`
      limits. Default if unruled: the #1754 policy (`DECIMAL_MAX_UNSCALED_BYTES = 32 KiB` refusal;
      exponent form above `DECIMAL_POSITIONAL_MAX_BYTES = 1024` or `|scale| > 1_000_000`).
- [x] Move Node's `decimal_to_string` body (`bindings/node/src/value.rs:364-430`) into the crate,
      changing only the error type: `Result<String, DecimalError>` with a `Display` that is the single
      canonical corruption message. Keep the O(1) length check before the single base-10 conversion and
      the `i64` widening that makes `scale == i32::MIN` safe.
- [x] Unit-test every case the spec enumerates: empty; `scale == 0`; positive `scale` shorter/equal/
      longer than the digit count; negative `scale`; `i32::MIN`/`i32::MAX` scale; negatives of each;
      magnitudes at and just past the positional boundary; at and just past the refusal ceiling.
- [x] **3c. Record the behaviour delta.** Write down, with concrete inputs, every case whose observable
      Python behaviour changes (e.g. 2000-byte unscaled, `scale = 3`: raises today, renders after).
      This text feeds `CHANGELOG.md` and the Python README in task 9.
- [x] Commit.

## 4. VARINT (surface: `cqlite-ffi-common/src/varint.rs`)
- [x] `pub fn varint_to_bigint(&[u8]) -> num_bigint::BigInt` — empty ⇒ 0, big-endian two's complement,
      sign-extending at any length. This is the single semantic implementation.
- [x] `pub fn varint_to_sign_and_le_words(&[u8]) -> (bool, Vec<u64>)` — **derived** from the `BigInt`
      via `to_u64_digits()`, never computed independently. This is napi's
      `create_bigint_from_words(sign_bit, words)` contract.
- [x] Unit-test: empty; 1 positive byte; 1 negative byte; exactly 8 bytes; 9 and 17 bytes both signs;
      `i64::MIN`/`i64::MAX`; a value whose negation carries across a word boundary. Add a property-style
      case asserting that reassembling `(sign, words)` reproduces the `BigInt` for every test input.
- [x] Commit.

## 5. Error contract MOVE (surface: `cqlite-ffi-common/src/error_contract.rs`, `cqlite-core/src/lib.rs`)
- [x] `git mv cqlite-core/src/ffi_error_contract.rs cqlite-ffi-common/src/error_contract.rs` and
      `git mv cqlite-core/tests/ffi_error_contract_table.rs cqlite-ffi-common/tests/error_contract_table.rs`.
      Keep the type names (`FfiErrorRow`, `FfiErrorVariant`, `PyExceptionClass`, `contract_for`,
      `variant_of`) **unchanged** so the binding diff is import lines only. Update the module's own
      "Relocation" doc paragraph — it currently promises this move; replace it with a statement that the
      move has happened and why the crate has no FFI dependency.
- [x] Delete `pub mod ffi_error_contract;` from `cqlite-core/src/lib.rs:17`. **Add no re-export**
      (design D5): one item, one path.
- [x] **5b. OQ1 — only if the owner rules "rename in scope":** rename
      `cqlite_core::observability::error_schema::ErrorCategory` → `ObsErrorCategory` and update its call
      sites. Leave `cqlite_core::error::ErrorCategory` and `cql/error.rs`'s local enum alone. If the
      ruling is "separate follow-up", do nothing here and make sure #1705 carries the note.
- [x] Re-point consumers: `bindings/python/src/error.rs:25`, `bindings/python/src/lib.rs:108`,
      `bindings/node/src/error.rs:65`, `bindings/node/src/lib.rs:83,115`, and the prose at
      `bindings/node/README.md:284`. Grep the whole repo afterwards to confirm zero stragglers.
- [x] `cargo test -p cqlite-ffi-common` — all 12 moved tests pass unmodified.
- [x] Commit.

## 6. INET (surface: `cqlite-ffi-common/src/inet.rs`)
- [x] `InetKind { V4, V6 }`, `InetError { len }` with the ONE `Display` spelling of
      `"Invalid inet address length: {n} (expected 4 or 16)"`, `inet_kind`, `inet_bytes_to_string`.
- [x] Move the two inet unit tests from `bindings/python/src/value.rs:643-657` here and **delete** the
      `#[cfg(test)] fn inet_to_string` at `value.rs:599` — a third formatter whose hex fallback
      contradicts production behaviour.
- [x] Unit-test 4-byte, 16-byte (including a compressible IPv6), 0-byte, 5-byte, 15-byte, 17-byte.
- [x] Commit.

## 7. OTel keys (surface: `cqlite-ffi-common/src/otel_keys.rs`)
- [x] `pub const KNOWN_OTEL_KEYS: &[&str]` with the 7 snake_case names currently at
      `bindings/python/src/observability.rs:85-106`. Document that the list is the snake_case spelling
      (Python dict keys / Rust field names), and that Node's JS-visible names are the camelCase
      `#[napi(js_name)]` forms.
- [x] Commit.

## 8. Vector tables (surface: `cqlite-ffi-common/src/vectors.rs`)
- [x] Export `DECIMAL_VECTORS`, `VARINT_VECTORS`, `INET_VECTORS` as plain `pub const` data (no feature
      gate — the bindings' test builds must reach them). Each entry: a name, the input, and the single
      expected rendering or expected error message.
- [x] Cover every edge case enumerated in tasks 3, 4 and 6, including at least one expected-error entry
      per table.
- [x] Assert inside the shared crate that every vector's expectation matches the shared implementation,
      so a wrong vector is caught in the crate rather than only in a binding suite.
- [x] Commit.

## 9. Wire the Python binding (surfaces: `bindings/python/src/value.rs`, `observability.rs`, `lib.rs`, `Cargo.toml`)
- [x] Add `cqlite-ffi-common = { path = "../../cqlite-ffi-common" }`.
- [x] `decimal_to_pydecimal` becomes: call `decimal_to_string`, map `DecimalError` →
      `cqlite_core::Error::corruption` → existing `to_py_err`, wrap the `Ok` string in
      `decimal.Decimal(str)`. Delete the digit-cap block, the `sys.get_int_max_str_digits()` probe, the
      `str()` round trip and all the `format!` scale logic (`value.rs:307-435`).
- [x] `varint_to_pyint` routes through `varint_to_bigint` per design D2 (prefer enabling pyo3's
      `num-bigint` feature **in this binding's manifest**; fall back to `int.from_bytes` over
      `to_signed_bytes_be()` if it is not abi3-compatible on pyo3 0.23). Either way the semantic comes
      from the shared fn.
- [x] `inet_to_py` calls `inet_kind` for the dispatch and renders `InetError` via `Display` for the
      error message. No literal copy of the message text remains.
- [x] `observability.rs` consumes `KNOWN_OTEL_KEYS`; behaviour (both `ValueError` paths and the message
      naming the recognised keys) unchanged.
- [x] Add the internal `_ffi_common_vectors_*` test-support functions that render every vector through
      the production path, registered in `_cqlite` alongside the existing `_decimal_from_parts` /
      `_inet_from_bytes` (`lib.rs:131`).
- [x] Add `bindings/python/tests/test_shared_vectors.py` asserting the full tables.
- [x] Commit.

## 10. Wire the Node binding (surfaces: `bindings/node/src/value.rs`, `observability.rs`, `lib.rs`, `Cargo.toml`, `__test__/`)
- [x] Add `cqlite-ffi-common = { path = "../../cqlite-ffi-common" }`.
- [x] Delete local `decimal_to_string` + its three policy consts, `varint_to_bigint`'s body (keep only
      the `create_bigint_from_words` adapter over `varint_to_sign_and_le_words`), and
      `inet_bytes_to_string`; `inet_to_string_js` now wraps the shared fn and maps `InetError` via
      `Display`. Keep `duration_to_object` and the `Value::Date` arm **as-is** (design D10).
- [x] Add the `#[cfg(test)]` set-equality assertion between `OtelOptions`'s snake_case field names and
      `KNOWN_OTEL_KEYS`, failing in both directions.
- [x] Add the `_ffiCommonVectors*` test-support surface mirroring Python's, re-exported from
      `lib/index.js` in the established `_errorContract*` style, and
      `__test__/shared-vectors.test.js` asserting the full tables.
- [x] Keep `num-bigint` in the Node manifest only if still used directly; remove it if the shared crate
      is now the only consumer.
- [x] Commit.

## 11. Cross-binding + regression verification
- [x] `cargo test -p cqlite-ffi-common`.
- [x] `cd bindings/node && npm run build && npm test` — all pre-existing suites green, plus the new
      vector suite.
- [x] `cd bindings/python && maturin develop` then
      `env CQLITE_DATASETS_ROOT=<the exact root the fetch script prints> pytest tests -v` — all
      pre-existing suites green, plus the new vector suite. Dataset-backed tests must FAIL LOUDLY on
      present-but-empty data, never skip.
- [x] **Mutation-verify the wiring claim** (spec: "a binding's rendered output tracks a change made only
      in the shared crate"): in a scratch tree, perturb a shared rendering function and confirm BOTH
      binding suites go red. Record it in the PR body — this is the evidence that the extraction is real
      and not a copy with a new address.
- [x] Re-check `bindings/python/tests/test_decimal_guard.py` and `__test__/abort-safety.test.js` against
      the OQ3 ruling; update expectations only where the ruling changes behaviour, and say so in the PR.

## 12. Docs + doctrine in the same change
- [x] `CHANGELOG.md`: the DECIMAL policy convergence (the only user-visible change) and the
      `cqlite-core::ffi_error_contract` removal.
- [x] Binding READMEs: the DECIMAL rendering policy, stated once and identically in both.
- [x] `bindings/node/README.md:284` re-pointed to the new path.
- [x] Note in the PR body that the `pub-surface` gate component does **not** detect public-API removal
      (#1712/#3366), so the `cqlite-core` module removal is being surfaced in prose deliberately.

## 13. Review, gate, close
- [x] `scripts/agent-gate.sh --lite` green on every fix round (summary-file redirect).
- [x] `rust-reviewer` + roborev on the lite-green diff, BEFORE the full gate
      (`bash scripts/flow/roborev-review.sh --agent codex --model gpt-5.6-sol --repo <abs>`), blockers
      fixed and re-reviewed; nits batched into one linked follow-up issue.
- [x] Open the PR; hand the endgame to `flow-closer`: ONE full `scripts/agent-gate.sh` (paste the
      `AGENT-GATE SUMMARY` verbatim, verify `RESULT: PASS` **and** `tree-integrity: PASS`) → `spec-auditor`
      C intent audit against `specs/binding-shared-scalar-math/spec.md` → final roborev → pre-merge SHA
      assert → `gh pr merge --auto --squash --delete-branch` → `flow-finalize`.

## Implementation notes (filled in during delivery)

- **OQ1 = (a)** applied: `observability::error_schema::ErrorCategory` →
  `ObsErrorCategory`, 93 call sites across 9 files (including the
  `classify()`-source-scanning test's `ARM` needle). `error::ErrorCategory` and
  `cql/error.rs`'s local enum untouched.
- **OQ2 = (a)** applied: the table moved as-is, still mirroring `Error::category()`.
- **OQ3 = (a)** applied: the #1754 policy for both bindings.
- **Two defects found while extracting, both fixed here:**
  1. The #1754 positional branch fed `scale` to a `{:0>width$}` format spec, but
     `core::fmt` packs the width into a `u16` — so a WELL-FORMED value with
     `65535 < |scale| <= DECIMAL_MAX_SCALE_DIGITS` PANICKED
     ("Formatting argument out of range") on the Node render path. Padding is now
     built explicitly. Found by the spec's mandated scale sweep.
  2. `bindings/node/__test__/prepared.test.js` asserted an EXACT whole-object
     export match and was ALREADY RED on `origin/main` (`lib/index.js` has
     exported `_errorContractProbe`/`_errorContractNodeCodes` since #1451).
     Re-scoped to the PUBLIC surface + an assert that every remaining key is
     underscore-marked internal.
- **`scripts/generate-loader.mjs` carries a hand-maintained `IDENTS` list**: a
  `#[napi]` function absent from it is silently unreachable from JS even though
  its symbol is in the `.node`. `ffiCommonRenderVectors` was added there.
- **VARINT route taken: design D2 route 1** — pyo3's `num-bigint` feature compiles
  under the pinned `abi3-py39` build, so the shared `BigInt` goes straight to
  Python with no byte round trip.
