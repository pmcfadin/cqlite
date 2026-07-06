# Doc deltas for agent-gate changes (issues #2078, #2081)

These are the documentation edits the two gate changes require. They are recorded
here (not applied) because the implementing worktree may not modify CLAUDE.md, the
`agents-developing/` site, or `docs/` beyond this scratch file. Apply them when the
gate changes land.

## Issue #2078 — FULL gate fails closed on an absent dataset corpus

- **CLAUDE.md → "Essential Commands" / gate description**: add a note that the FULL
  `scripts/agent-gate.sh` now **FAILs CLOSED** when the *fetched validation corpus*
  (`test_basic/...`) is absent, even though a fresh worktree's ~19 committed
  byte-parity reference `*-Data.db` keep the raw Data.db count > 0. Previously the
  dataset-dependent components SKIPped and the gate returned a false PASS.
  - Opt-out: `AGENT_GATE_ALLOW_MISSING_FIXTURES=1` restores the lenient SKIP and
    stamps a machine-checkable `missing-fixtures: OPT-OUT (...)` line into the SUMMARY
    block, so an intentional opt-out is visible in the pasted artifact.
  - `--lite` and `--only` are unchanged (lenient). Behavior with the corpus present is
    byte-identical.
  - Remedy line emitted on FAIL: `bash test-data/scripts/fetch-datasets.sh` (or point
    `CQLITE_DATASETS_ROOT` at a checkout that has the corpus).
- **gate-contract page** (`agents-developing/gate-contract/`): document the two new
  machine-checkable SUMMARY lines:
  - `missing-fixtures: FAIL-CLOSED (#2078)` — on a corpus-absent FULL gate (RESULT: FAIL).
  - `missing-fixtures: OPT-OUT (AGENT_GATE_ALLOW_MISSING_FIXTURES=1) ...` — on an
    opt-out run (dataset coverage SKIPPED; this run does NOT validate dataset-backed
    correctness).

## Issue #2081 — `--delta` executes node `__test__/` and `scripts/tests/*.sh`

- **CLAUDE.md → the `--delta` doc block** (and the matching `Agent-team conventions`
  bullet): **remove `node __test__/` files and `scripts/tests/*.sh` from the
  refusal list.** `--delta` now EXECUTES them:
  - `bindings/node/__test__/*` → runs the jest suite scoped to the changed files
    against the **already-built** native module. **Fail-closed design point:** if the
    native module is not built (or node/npm is unavailable), `--delta` **REFUSES** the
    re-cert (it NEVER builds with cargo and never passes vacuously) — build it first
    (`cd bindings/node && npm run build`) or run the full gate.
  - `scripts/tests/*.sh` → executes exactly the changed self-test scripts.
  - Everything else stays refused (src, `Cargo.*`, workflows, config, test-data, and
    any `.rs` that is not a Cargo `--test` target).
  - The DELTA SUMMARY now carries a `delta-executors: ...` line naming which executors
    ran (e.g. `scoped-tests(rust/python) node-tests(2) shell-selftests(1)`).
- Update the current CLAUDE.md refusal wording:
  `... which --delta's components never execute) REFUSES the re-cert ...` — drop the
  `node __test__/ files and scripts/tests/*.sh` clause from the parenthetical, since
  they are now executed (node with the build-ready precondition).
