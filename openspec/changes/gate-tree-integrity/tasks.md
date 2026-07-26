# Tasks — gate-tree-integrity (issue #2926)

## 1. Tree-identity capture (surface: `scripts/agent-gate.sh`, new `_tree_identity`)
- [ ] 1.1 TDD first: add `scripts/tests/test_agent_gate_tree_integrity.sh` case **F** (capture
      idempotence + no index/worktree/ODB perturbation) and case **C** (append to an
      already-modified tracked file changes the digest while `git status --porcelain` does not).
      Watch C fail against a deliberate porcelain-only prototype before writing the real digest.
- [ ] 1.2 Implement `_tree_identity <out-manifest>`: NUL-framed per-path manifest
      (`H` head, `T` tracked-vs-HEAD with working-tree blob sha / `DELETED` / mode, `U` untracked
      non-ignored with blob sha), `LC_ALL=C sort -z` ordering, `sha256` digest. All git calls
      `--no-optional-locks`; `git hash-object --stdin-paths` **without** `-w`.
- [ ] 1.3 Untracked hash cap `AGENT_GATE_TREE_HASH_CAP_BYTES` (default 8 MiB) with
      `SIZE:<n>:MTIME:<ns>` fallback + a `tree-hash-cap:` stamp when non-default or used.
- [ ] 1.4 Exclusions: `--exclude-standard` plus the run's own `$SUMMARY_FILE` /
      `$SUMMARY_FILE.integrity-fail.*` when under `$REPO_ROOT`. No other path excluded.

## 2. Start capture + provenance stamps (surface: startup sentinel + every SUMMARY emit)
- [ ] 2.1 Capture the start identity immediately after summary-path resolution, before `run_lite`,
      `run_delta` and `acquire_gate_slot`; persist `$LOG_DIR/tree-identity.start`.
- [ ] 2.2 Add `tree-start:` to the startup `INCOMPLETE` sentinel (no `tree-end:` there); keep the
      sentinel's terminal line exactly `RESULT: INCOMPLETE (gate did not finish)`.
- [ ] 2.3 Add `tree-start:`, `tree-end:` and `tree-integrity:` to `SUMMARY_META` in the full gate,
      `run_lite`, `run_delta` and the `--only` path; synthetic `selftest` identity in
      `--emit-summary-selftest` and the lite-aggregation self-test.
- [ ] 2.4 Assert no added line contains the token `RESULT:` (#2908 non-regression) — extend
      `scripts/tests/test_agent_gate_summary.sh` with both poll predicates.

## 3. Verify + fail closed (surface: `record_result` boundary and the terminal emit)
- [ ] 3.1 `_assert_tree_integrity <component>` at the `record_result` chokepoint, next to
      `_assert_summary_integrity`; MAIN lane emits the named FAIL block and exits non-zero, SIDE
      lane appends to `$LOG_DIR/tree-integrity.fail` and returns non-zero (mirrors #2874).
- [ ] 3.2 `_apply_tree_integrity_marker` post-drain: force `OVERALL=FAIL` + the named terminal line.
- [ ] 3.3 Terminal capture immediately before `_emit_terminal_summary` (full, lite, delta) — the
      authoritative check; a mutation after the last boundary must still FAIL.
- [ ] 3.4 Named line content: `tree-integrity: FAIL (tree-mutated-midrun; head <a>→<b>; changed:
      <paths…> (+N more); detected-after-component: <c>)` built from the start/end manifest diff.
- [ ] 3.5 `Cargo.lock`-only difference → `tree-integrity: PASS (lockfile-settled: …)`; lockfile plus
      anything else → FAIL. File the follow-up issue for adding `--locked` to gate cargo calls.
- [ ] 3.6 Confirm coexistence with `summary-integrity`: both named lines, one `RESULT: FAIL`.

## 4. Self-test (surface: `tooling-tests` component)
- [ ] 4.1 Complete `scripts/tests/test_agent_gate_tree_integrity.sh` cases **A** (mutated → no
      certification, named line, non-zero exit) and **B** (unmutated control → certifies). A+B are
      the discrimination proof; no test-only bypass seam is introduced.
- [ ] 4.2 Cases **D** (untracked add/change/remove), **E** (target/ + `*.log` + summary churn →
      PASS), **G** (`--lite` and `--delta` mutated → neither certifies).
- [ ] 4.3 Hermeticity: per-run `mktemp …XXXXXX` fixtures, traps removing only this run's paths;
      passes the existing fixed-name/mktemp structural self-check.
- [ ] 4.4 Sequencing by rendezvous on `$LOG_DIR/<component>.result`, never a fixed sleep; no
      wall-clock assertion in the correctness path (#2642).
- [ ] 4.5 Wire the new file into `tooling-tests`; record the component's wall-clock before/after on
      the PR (target: within noise — the guard adds well under a second).

## 5. Doctrine (same change)
- [ ] 5.1 `website/src/content/docs/agents-developing/gate-contract.md`: add the three lines to both
      machine-checkable block renderings; add the "a mid-run tree mutation invalidates the run"
      section and the closer's `tree-integrity:` check.
- [ ] 5.2 `CLAUDE.md` gate section: one line stating the invalidation rule and the closer check.
- [ ] 5.3 `docs/development/gate-ops.md`: the closer+fixer overlap shape (#1582/#1930) and recovery.
- [ ] 5.4 Note the stated limitation (gitignored dataset inputs are outside the digest; covered by
      the existing `datasets:`/`ci-pins:` stamps).

## 6. Quality stages
- [ ] 6.1 `--lite` each fix round (summary-file redirect, unique path).
- [ ] 6.2 Review-first: `rust-reviewer` (shell/script scope) + roborev on the lite-green diff.
- [ ] 6.3 Open PR; `flow-closer`: ONE full gate of record → C intent audit (`spec-auditor` against
      this change's `specs/**`) → final roborev → merge-on-green → finalize.
- [ ] 6.4 Field verification: the full gate of record for THIS PR must itself emit
      `tree-integrity: PASS` with matching `tree-start:`/`tree-end:` digests — the change certifies
      itself. Record the block on issue #2926.
