# Tasks: #3630 — a column name SHALL NOT reach JavaScript's object model

Ordered so every later task has a red test to turn green. Surfaces named per `openspec/config.yaml`.

## 1. Subject first (TDD red)

- [ ] 1.1 Measure whether CQLite's query surface supports `SELECT id AS __proto__`. Record the
      answer in the PR. If yes, it is an ADDITIONAL subject, never a substitute for 1.2.
      *Surface*: `Database.executeNative` / `execute`.
- [ ] 1.2 Add `test-data/schemas/issue-3630-row-collision.cql` declaring `row_collide` with quoted
      `"__proto__"`, `"constructor"`, `"toString"` plus `real_col`, uncompressed, per `design.md` D2.
- [ ] 1.3 Extend/clone `test-data/scripts/generate-issue-3504-udt-collision.sh` into
      `generate-issue-3630-row-collision.sh` (cassandra:5.0.2, `cqlsh` INSERT + `nodetool flush`,
      `USING TIMESTAMP 1000` on every INSERT). Three rows: all-set, `"__proto__"` NULL, none-set.
- [ ] 1.4 Commit the fixture **checkout-relative** under `test-data/fixtures/issue_3630/`
      (`git add -f` the gitignored binaries; verify from a fresh `git worktree add --detach HEAD`,
      not the dirty tree) with a `README.md` recording the measured before/after and a
      `binding-parity-facts.json`, mirroring `test-data/fixtures/issue_3504/`.
- [ ] 1.5 Write `bindings/node/__test__/issue-3630-row-key-namespace.test.js` — every scenario of the
      spec's first requirement, fail-closed on fixture absence (never `skip`; the fixture is
      committed source), asserting key **sets** not counts, and asserting the extras case **reached**
      the extras path. *Surface*: the Node public query surface. **Must be RED.**
- [ ] 1.6 Measure the `Value::Json` route of `design.md` D3 (Cassandra-written `text` + committed
      CQLite schema declaring the column `json`) and record the result. Write the JSON scenarios in
      the same file if reachable; otherwise record the unreachability measurement and cover
      `json_to_napi` in `bindings/node/src/value_tests.rs`. **Must be RED.**

## 2. The row mechanism

- [ ] 2.1 Implement M1 (`JsObject::define_properties` with `Property`, attributes
      writable|enumerable|configurable) for **both** `row_to_object` paths. Preserve the
      present-column-only skip and the name-sorted extras order. An unrepresentable name is an `Err`
      through `to_napi_error`, never a skipped column. *Surface*: `bindings/node/src/value.rs`.
- [ ] 2.2 Doc comment at the write site recording: the `[[Set]]`/prototype-chain mechanism, the
      measured two failure modes, why this is NOT a `__proto__` literal check, and the accepted cost
      (`'toString' in row` stays true; `Object.hasOwn` is the correct probe) — the standard
      `udt_to_object` sets at `value.rs:479-509`.
- [ ] 2.3 **Measure** per `design.md` **D1b**, whose decision rule is FIXED BEFORE MEASUREMENT (lead
      condition on 3630-R1): `test_wide_rows` scan through the Node binding, 1 warmup + 7 timed runs,
      alternated baseline/candidate; check the **validity precondition first** (baseline relative
      half-range ≤ 2.5%); then adopt M1 iff median regression ≤ **5%**, else **M2**. Quote the rule
      AND the numbers in the PR body. Never widen the threshold to fit a noisy harness.
- [ ] 2.4 Rust unit coverage in `bindings/node/src/value_tests.rs` for whatever is unit-reachable
      (descriptor construction, the unrepresentable-name refusal). *Component*: `binding-rust-tests`.

## 3. The JSON mechanism

- [ ] 3.1 Build every `json_to_napi` object with `ConvCtx::create_null_prototype_object()` (the
      #3504 helper, already cached per result), at every nesting depth.
- [ ] 3.2 Doc comment recording the DIFFERENT contract from rows and why (a JSON object is a data
      mapping, like the UDT field bag; a row has a declared column list).

## 4. Declared surface + Python assertion

- [ ] 4.1 Update `bindings/node/lib/index.d.ts`: `Row`'s contract + absence-probe guidance; add the
      JSON-object shape to `Value`, documented as null-prototype. Keep
      `__test__/typescript-definitions.test.js` green. *Surface*: the published type declarations.
- [ ] 4.2 Add the Python row-path assertion (AC8) against the same fixture in
      `bindings/python/tests/`. No Python behaviour changes.
- [ ] 4.3 Update `docs/development/M4_spec.md`'s **b-5** row and any binding-parity doc the change
      touches; add the CLAUDE.md/website note only if this changes agent-facing doctrine (it does
      not — no new gate component, no new invocation rule).

## 5. Certification

- [ ] 5.1 `--lite` green each fix round, summary-file redirect:
      `AGENT_GATE_SUMMARY_FILE=/tmp/gate-3630-lite.txt bash scripts/agent-gate.sh --lite > /tmp/lite.log 2>&1 < /dev/null`.
- [ ] 5.2 `rust-reviewer` + sanctioned roborev
      (`scripts/flow/roborev-review.sh --agent codex --model gpt-5.6-sol`) on the lite-green diff,
      BEFORE any full gate. Blockers fixed and re-reviewed; nits batched into one follow-up issue.
- [ ] 5.3 Open the PR with `Closes #3630`, the two measurements (2.3 rows/s, 1.6 JSON reachability)
      and the recorded mechanism decision.
- [ ] 5.4 `flow-closer`: ONE full `scripts/agent-gate.sh` (the gate of record — `node-bindings` runs
      the whole jest suite and `binding-rust-tests` runs cqlite-node's Rust tests since #3522, so both
      new suites execute) → `spec-auditor` C audit against this spec → final roborev →
      `scripts/flow/premerge-assert.sh` → `gh pr merge --auto --squash --delete-branch`.
- [ ] 5.5 `flow-finalize`: `openspec archive`, telemetry record, worktree/branch/claim cleanup,
      `CronDelete drive-issue-3630`, closing comment.
