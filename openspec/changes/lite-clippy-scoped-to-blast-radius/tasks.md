# Tasks: `--lite` clippy scoped to the blast radius

Each task names the surface or test it exercises.  Tasks are ordered so the measurement that could
falsify the change runs before the change is built out.

## 1. Baseline measurement, before any behaviour change

- [ ] 1.1 Record the unscoped baseline at a named `origin/main` sha.  Surface: `scripts/agent-gate.sh --lite`.
      Three populations, cold and part-warm each: a narrow non-core diff, a `cqlite-core/src/` diff, and
      a docs-only diff.  Capture elapsed clippy seconds and the package count linted.
- [ ] 1.1b Measure `FLOOR` alone as its own row, since its cost is paid on every lite run regardless
      of diff and therefore bounds the best case.  If `FLOOR` alone approaches the unscoped time, this
      change cannot deliver a sub-two-minute inner loop and the artifact says so.
- [ ] 1.2 Write `docs/round-artifacts/lite-clippy-scope-measurements.md` with the baseline table and
      the exact commands, so every later figure is re-derivable from the repo.
- [ ] 1.3 **Decision point.** If the `cqlite-core/src/` population shows the scoped set would include
      substantially the whole workspace, record that in the artifact and say so in the proposal's Why.
      Do not proceed as though the change helps that class when the measurement says it does not.

## 2. Expose the existing blast-radius derivation as a reusable scope

- [ ] 2.1 Extract the `scoped-tests` package-set derivation into a named function with no side
      effects, returning the set plus a measurable success or a named failure cause.  Surface:
      `scripts/agent-gate.sh`.
- [ ] 2.2 Assert the extraction is behaviour-preserving for `scoped-tests`.  Test:
      existing `scripts/tests/test_agent_gate_component_set.sh` stays green, plus a new case pinning
      the derived set for a fixed synthetic diff.
- [ ] 2.3 Confirm no second definition of the derivation exists.  `scripts/agent-gate.sh:6459`
      records that a duplicated `_tree*` function previously FAILed `tooling-tests` through a
      uniqueness assert; the same trap applies here.

## 3. Declare FLOOR

- [ ] 3.1 Define `FLOOR` in exactly one place, with `cqlite-py` as its founding entry and #1893 cited
      at the definition site.  Surface: `scripts/agent-gate.sh`, adjacent to `run_clippy` at `:11193`.
- [ ] 3.2 Write the rule into the definition's comment: an entry needs a named reason or it does not
      go on the list.  A floor that grows undisciplined rebuilds the whole-workspace matrix.

## 4. Scoped clippy at the `--lite` call site only

- [ ] 4.1 Add the scoped mode to `run_clippy`, filtering the four-stage matrix by
      `blast_radius ∪ FLOOR` and preserving each package's own feature string.  Surface:
      `scripts/agent-gate.sh:11193`, reached from `run_lite`'s call at `:20546`.
- [ ] 4.2 Preserve stage 1's exclusion of `cqlite-core`, `cqlite-cli`, `cqlite-flight`, `cqlite-py`
      and `cqlite-node`, so `--all-features` still never activates a duckdb or otel feature.  Do not
      re-derive that exclusion set from the scope.
- [ ] 4.3 Leave the full gate's call to `run_clippy` on the whole-workspace path.  Test: a full-gate
      summary carries no scoped disclosure.
- [ ] 4.4 Make an unmeasurable derivation a `SKIP` naming the cause, never a `PASS` and never an
      empty-set lint.  A computed empty scope is evidence of breakage, since `FLOOR` is non-empty.
- [ ] 4.5 Leave `--delta` and `--only` unchanged.  `--only clippy` keeps the whole-workspace matrix.
      Test: their summaries are unchanged in shape for a fixed input.
- [ ] 4.6 Confirm `CQLITE_CLIPPY_FULL=1` at `:11194` still short-circuits to the historical
      `--workspace --all-targets --all-features` pass and is not reached via the scoped path.

## 5. Disclosure in the LITE SUMMARY

- [ ] 5.1 Emit the CHECKED / NOT CHECKED counts and the log pointer through the existing
      `_status_detail` boundary.  Surface: the `AGENT-GATE LITE SUMMARY` block.
- [ ] 5.2 Write `0 NOT-CHECKED RECOGNISED` for a full-coverage scoped run.  Test: a synthetic diff
      whose blast radius is the whole workspace.
- [ ] 5.3 Write the excluded package names to `<logdir>/clippy.log` and to nowhere else.
- [ ] 5.4 Verify a package path carrying a control character or `RESULT:` cannot break the summary
      grammar or the completion probe.  Test: a synthetic package path fixture.

## 6. Discriminating regression test

- [ ] 6.1 Write `scripts/tests/test_agent_gate_lite_clippy_scope.sh` and wire it to `tooling-tests`.
- [ ] 6.2 Pin mutation 1: scoped set replaced by the empty set must red the test.
- [ ] 6.3 Pin mutation 2: disclosure line removed must red the test.
- [ ] 6.4 Pin mutation 3: #2658 fan-out dropped for a `cqlite-core/src/` path must red the test.
- [ ] 6.5 Pin mutation 4: `cqlite-py` removed from `FLOOR` must red the test.  Assert via the #1893
      route — an uncompilable `bindings/python/src` plus a non-python diff must not reach
      `OVERALL=PASS`.
- [ ] 6.6 Apply every mutation in the test's own scratch copy.  Verify the gate reports
      `tree-integrity:` clean and `dirty: no` on a run that includes this test.
- [ ] 6.7 Register the new test in the component manifest so
      `scripts/tests/test_agent_gate_component_set.sh`'s staleness guard stays green.

## 7. Post-change measurement

- [ ] 7.1 Re-run the populations from task 1 against the scoped implementation.  Append the scoped
      column to `docs/round-artifacts/lite-clippy-scope-measurements.md`.
- [ ] 7.2 Record the discriminating case: a clippy violation planted outside `blast_radius ∪ FLOOR`.
      Scoped lite must not FAIL, must name it unchecked, and the full gate must FAIL.
- [ ] 7.3 State the residual in the artifact: a violation in a package that is neither touched, nor a
      direct dependent, nor on `FLOOR` is caught by the gate of record, not by lite.

## 8. Doctrine

- [ ] 8.1 Correct `CLAUDE.md`'s Lite row: `blast_radius ∪ FLOOR` default, `CQLITE_CLIPPY_FULL=1`
      escape retained, bands cited from the measurement artifact rather than restated.
- [ ] 8.2 Remove the stale `:17233` / `:18220` two-site framing from that row.  There is one
      `run_clippy` definition, at `:11193`, called by both modes.
- [ ] 8.3 Resolve `run_lite`'s self-contradiction: its comment at `:20529` says "FULL-workspace
      clippy" while its printed banner at `:20539` says "scoped workspace clippy."
- [ ] 8.4 Check `docs/development/gate-ops.md` for text the change makes wrong and correct it here.

## 9. Certification

- [ ] 8.1 `rust-reviewer` + `scripts/flow/roborev-review.sh --agent <a> --model <m>` on the lite-green
      diff, before the first full gate (review-first, #2086).
- [ ] 8.2 Run `scripts/agent-gate.sh` — the full gate of record, once, via the summary-file redirect:
      `AGENT_GATE_SUMMARY_FILE=/tmp/gate-summary.txt bash scripts/agent-gate.sh > gate.log 2>&1 < /dev/null`.
      Paste the `AGENT-GATE SUMMARY` block verbatim.
- [ ] 8.3 **Self-certification check:** confirm this change's own `--lite` run printed the scoped
      disclosure line.  `agent-gate.sh` is read from the checkout, so this PR can and must exercise
      itself.
- [ ] 8.4 `spec-auditor` (C) against `openspec/changes/lite-clippy-scoped-to-blast-radius/specs/**`,
      after the gate is green.  Every requirement `satisfied` with a named test as evidence.
- [ ] 8.5 Final `roborev` round, last, after C.  Any non-PASS terminal `RESULT` blocks the merge.
- [ ] 8.6 `scripts/flow/premerge-assert.sh <pr> <certified-sha>`, re-read comments for a fresh
      `HOLD:`, then arm `gh pr merge --auto --squash --delete-branch`.
- [ ] 8.7 `flow-finalize`: archive the change, stamp the delivery-telemetry record, close the issue.
