# Tasks: schemas-root-contract (issues #3148, #3131)

> Design decided in `design.md`: the committed schema fixtures resolve **checkout-relative**
> (anchored on the WORKSPACE-ROOT `Cargo.toml` found by walking `CARGO_MANIFEST_DIR`'s ancestors — see
> §7 N4; the first cut keyed that walk on `test-data/schemas` and was corrected in review), never
> from `CQLITE_DATASETS_ROOT` — #3148's proposed
> fix 4, taken as the owner's decision (AC (h)). One shared `#[path]`-included file hosted under
> `test-data/support/` because it encodes the layout of `test-data` itself and is owned by neither crate.
> The gate preflight becomes a belt-and-braces per-FILE readability assert with no opt-out.
> AC → requirement map is at the top of `specs/test-fixture-roots/spec.md`.

## 1. The single roots contract (surface: `test-data/support/fixture_roots.rs`)
- [x] Create the shared std-only module. Delivered surface (as merged, after review round 1):
      `datasets_root`, `datasets_root_if_present`, `sstables_root`, `schemas_root`,
      `schemas_root_resolved`, `resolve_schemas_root` (the pure, env-free resolver), `schema_path`,
      `readable_file`, `checkout_test_data_dir`, `workspace_root`, `workspace_root_from`. A
      `check_schema_files` helper existed in the first cut and was DELETED in review round 1 (§7 N6):
      zero callers, zero tests, no wiring evidence.
- [x] Resolve the checkout by walking `CARGO_MANIFEST_DIR`'s **ancestors** (not a hardcoded
      `../test-data`), so a crate nested deeper than one level still resolves and no `..` component is
      ever handed to the kernel. **The walk keys on the workspace-root `Cargo.toml` marker, NOT on
      `test-data/schemas`** — the fixtures-keyed version shipped in the first cut and was replaced in
      review round 1 (§7 N4) because it let a sparse checkout, or a worktree nested inside another
      checkout, silently resolve to the OUTER checkout's fixtures.
- [x] Honor `CQLITE_SCHEMAS_ROOT` only when set, non-blank, **ABSOLUTE** AND a readable directory; fall
      through to the checkout for an absolute-but-unusable value (a stale export must degrade, not pin
      every load to an unusable path). A RELATIVE value is REJECTED fail-closed rather than resolved —
      added in review round 1 (§7 B1).
- [x] Treat an exported-but-EMPTY env value as unset for both roots (a scripting accident, never an
      intentional root).
- [x] `schema_path` verifies readability and panics naming the resolved ABSOLUTE path, the root, the
      resolution source, that these are committed source, and the remedy.
- [x] Document the two-shape `datasets_root()` contract in the module docs with the stated reason for
      each shape (#3148 AC (e)), including why the fallible shape has no checkout fallback.
- [x] `#![allow(dead_code)]` at module scope — the file is `#[path]`-included into ~14 targets, each
      using a subset.

## 2. Migrate all four call sites (#3148 AC (d))
- [x] `cqlite-core/benches/fixtures/mod.rs` — include the shared module as `pub mod roots`; reduce
      `datasets_root`/`sstables_root`/`schemas_root` to thin delegations; switch `open_read_db_with_config`
      to `roots::schema_path` so an unreachable fixture is diagnosed at the root, not inside ingest.
- [x] `cqlite-core/tests/dead_cache_delete_tests.rs` — replace the local fallible `datasets_root()` with
      `use fixture_roots::datasets_root_if_present as datasets_root;` and the `join("../schemas")` with
      `schema_path`; drop the now-redundant `datasets_root()?` in `open_fixture_db` (its `data_db(..)?`
      already short-circuits identically).
- [x] `cqlite-core/tests/observability_correctness.rs` — delegate the third `datasets_root()` copy and
      switch to `schema_path`.
- [x] `cqlite-cli/benches/export_csv.rs` — include the shared module (symmetric `../../` path, no
      cross-crate reach into cqlite-core's bench internals) and switch to `schema_path`; update the
      module doc that declared the duplication deliberate so it now scopes that to fixture-OPEN logic only.
- [x] Verify zero open-coded `join("../schemas")` expressions remain in Rust code.
- [x] Compile all migrated targets under `RUSTFLAGS="-D warnings"`, including the `cli-helpers`-gated ones
      and the `cqlite-cli` bench.

## 3. Gate preflight (surface: `scripts/agent-gate.sh`) (#3148 ACs (a) (b) (g))
- [x] Add `CANONICAL_SCHEMA_FILES` (the 6 `.cql` the dataset-backed components consume) and
      `SCHEMAS_LINE`.
- [x] Add `_gate_schemas_root` / `_gate_schemas_root_source` mirroring `schemas_root_resolved()` exactly.
- [x] Add `_missing_schema_files` (per-FILE readability) and the PURE `_schemas_status` (`OK|FAIL`,
      returning OK for `--lite`/`--only`).
- [x] Add `apply_schemas_preflight`: stamp the positive `schemas:` line on OK; on FAIL emit
      `missing-schemas: FAIL-CLOSED (#3148)` plus a remedy naming the exact absolute path and both fix
      commands, then exit 1. No opt-out.
- [x] Call it immediately after `apply_fixture_preflight` inside `if selected_needs_datasets`, so the
      corpus cause is still reported first when both halves are missing.
- [x] Stamp `SCHEMAS_LINE` into BOTH the terminal `SUMMARY_META` assembly and the component-boundary
      block, guarded so a `--lite`/`--delta` boundary omits it rather than inventing it.
- [x] Add the hidden `--preflight-schemas` hook (STATUS/ROOT/SOURCE/MISSING), with an optional 2nd arg
      seeding `ONLY` so the `--only` leniency branch is assertable (the arg dispatch is one `case "$1"`).
- [x] Update the `tooling-tests` doc block and the file-header component description.

## 4. Positive-control self-test (surface: `scripts/tests/test_agent_gate_schemas_preflight.sh`) (#3148 AC (c))
- [x] Hook cases: OK on the checkout; FAIL naming all 6 on a schemas-less root; FAIL naming ONLY the
      absentees on a present-but-incomplete root.
- [x] Real FULL-gate case with a COMPLETE synthetic corpus: non-zero exit, `missing-schemas: FAIL-CLOSED
      (#3148)`, `RESULT: FAIL`, never `RESULT: PASS`, and no cargo run.
- [x] Marker-separability case: the schemas failure must not stamp `missing-fixtures:`.
- [x] Remedy case: the block names the absolute `.cql` path and both fix commands.
- [x] Leniency cases: `--lite` clean LITE block with no marker; `--only core-tests` decision is OK.
- [x] Symlink-independence case (#3148 AC (f)): identical resolved root across real / symlinked /
      nonexistent datasets roots — asserted behaviorally, not claimed.
- [x] Structural reintroduction guard: zero open-coded `join("../schemas")` expressions in Rust code,
      exempting doc comments.
- [x] Single-definition cases: the shared file exists with all three contract fns; all four sites include
      it; no bench / migrated test reads `CQLITE_DATASETS_ROOT` directly.
- [x] Scrub inherited `AGENT_GATE_SUMMARY_FILE` and `CQLITE_SCHEMAS_ROOT` so the test measures the
      committed contract, not the caller's shell.
- [x] Wire into the `tooling-tests` component.

## 5. `fetch-datasets.sh` usability guarantee (#3131 items 1-2)
- [x] Add `guarantee_usable_root`, called on BOTH the warm-skip and post-extraction paths: re-verify the
      content independently of the pin fast path, fail loudly with a remedy naming the pin to clear, and
      print the exact `export CQLITE_DATASETS_ROOT=<absolute path>` line the run guarantees.
- [x] Print a NOTE when the populated root differs from the checkout default, naming the already-set env
      var as the cause, so an operator cannot fall back to the documented default and get a corpus-less root.
- [x] Print a NOTE that the CQL schema fixtures are committed source resolved checkout-relative and are
      NOT a sibling of this root (#3148).
- [x] Add non-mutating `--verify-only`, so the failure path is exercisable and operators/CI get a cheap
      "is this root usable?" probe.
- [x] Leave `rm -rf "${DATASET_ROOT}"` and `restore_ci_tracked_dataset_files`' CI-only short-circuit
      untouched (#2878), with a code comment stating the boundary and a self-test case asserting it.
- [x] Self-test cases: hollow root exits non-zero with a remedy; a content-complete root exits zero and
      prints the verbatim export line; the #2878 boundary holds.

## 6. Verification
- [x] Hostile-layout run (the layout that made this fail): `CQLITE_DATASETS_ROOT` pointed at a scratch
      root holding the corpus with **no** `../schemas` sibling, `CQLITE_SCHEMAS_ROOT` unset — the
      previously-failing targets pass (`dead_cache_delete_tests` 8/8 incl. the 4 `stats_*`,
      `memory_budget` 3/3, `issue_1494_converter_alloc_budget` 1/1,
      `issue_2075_row_assembly_alloc_budget` 1/1), with `--features cli-helpers` (+ `dhat-heap,arrow` for
      the budget lanes) so the `cli-helpers`-gated targets are actually compiled rather than silently empty.
- [x] Non-vacuity control: with `CQLITE_SCHEMAS_ROOT` pointed at an empty directory, exactly the 4
      schema-consuming `stats_*` tests FAIL with the new actionable message — proving the passes above are
      real schema loads, not skips.
- [x] Negative control: a real FULL gate with a complete corpus and an unreachable schemas root exits 1 at
      the preflight stamping `missing-schemas: FAIL-CLOSED (#3148)`.
- [x] `scripts/agent-gate.sh --lite` PASS with the summary-file redirect.
- [ ] Doctrine pass (CLAUDE.md + the `agents-developing/test-data` page) — handled separately on this
      branch; the required facts are listed at the end of `design.md`.

## 7. Review round 1 (rust-reviewer blockers B1-B3, nits N4/N6/N7; roborev job 8 findings 1-3)
- [x] **B1** — reject a RELATIVE `CQLITE_SCHEMAS_ROOT` fail-closed on BOTH sides
      (`resolve_schemas_root` returns `Err`; `_gate_schemas_override_reject` FAILs the preflight with its
      own marker text and hint), so the gate can no longer stamp `schemas: … under <relative>` for a run
      whose test binaries resolved a different root. Also removes the "relative path labelled absolute"
      remedy line (AC (b)).
- [x] **B2** — `--verify-only` creates nothing: `canonicalize_dataset_root` skips `mkdir -p "${parent}"`
      under the probe and reports a nonexistent parent as "root unusable".
- [x] **B3** — strict argument parsing FIRST, before the pin load and before canonicalization: any
      unrecognized argument exits 2 with usage, so a typo can no longer fall through to `rm -rf`.
      Confirmed no existing caller passes an argument.
- [x] **N4 / roborev finding 1** — anchor the checkout on a MARKER (nearest ancestor `Cargo.toml`
      declaring `[workspace]`) instead of on `test-data/schemas`, so a sparse checkout or a
      nested-in-another-checkout worktree can no longer resolve to the OUTER checkout's fixtures; return
      the path whether or not it exists so a missing tree fails loudly. Gate mirrors the same walk. The
      `..`-free claim is now true by construction (`Path::parent`, never `join("..")`).
- [x] **N6** — deleted `check_schema_files` (zero callers, zero tests).
- [x] **N7 / roborev finding 2** — same readability question on both sides: `readable_file`
      (`is_file()` AND openable) in Rust, `[ -f ] && [ -r ]` in the gate. Expanding the 6-file canonical
      list is DEFERRED (see the requirement's scope note) — the list is the set whose absence produced
      the observed failures.
- [x] **roborev finding 3** — print the guaranteed export line with `printf %q` so it round-trips a path
      containing spaces or shell metacharacters.
- [x] `cqlite-core/tests/issue_3148_fixture_roots_contract.rs` (7 tests) pins the Rust half of the
      resolution table via the PURE `resolve_schemas_root`, with no env mutation.
- [x] Self-test grown 16 → 25 cases in this round (30 after §8 and the round-2 corrections): relative-override (hook + shapes + blank + FULL-gate emit + the
      no-relative-labelled-absolute assert), the directory-named-like-a-`.cql` trap, `--verify-only`
      non-mutation asserted on the FILESYSTEM with a NOT-pre-created parent (the earlier case pre-`mkdir`ed
      its root and was blind to B2), unrecognized-argument rejection asserted against a POPULATED root so
      a regression would have to delete a fixture the case then checks for, `--help`, and export-line
      round-tripping through `eval`.
- [x] N5 — scope the spec's single-definition requirement to the four hard-failing sites and record why
      the ~15 `parent()?.join("schemas")`-with-fallback sites are out of scope.

## 8. Review round 2 (docs-pass finding: a positive line asserting an unperformed check)
- [x] `apply_schemas_preflight` gained a LENIENCY early-return: under `--only`/`--lite` it stamps an
      explicit `schemas: not checked (<mode> is lenient, #3148 AC (g)) — this block asserts NOTHING
      about the schemas root` and returns. Previously `_schemas_status`'s unconditional OK under
      `--only` fell into the OK branch and stamped `schemas: 6/6 canonical .cql readable under <root>`
      for a check that never ran — #3148's misleading `STATUS: OK`, one mode over. Chose the explicit
      named non-check over silent omission: silence lets a reader of a pasted block assume the FULL
      contract held.
- [x] The same early-return also fixes a SECOND instance of the class: the REJECT branch was not
      governed by `_schemas_status`, so a relative override FAILed even a lenient `--only` run — the
      effectful guard diverging from the pure decision it is documented to consume. One mode check now
      governs both.
- [x] Hidden `--preflight-schemas-line [only-list]` hook drives the REAL `apply_schemas_preflight` and
      prints the stamped line, so the self-test observes the ACTUAL summary text (a real
      `--only core-tests` run would spend minutes in cargo before printing anything).
- [x] Three new positive controls, CONFIRMED FAILING before the fix (with the leniency early-return
      temporarily removed): the `--only` case then stamped `schemas: 6/6 canonical .cql readable under
      <empty dir>`, and the relative-override case exited 1. Both pass after; the third asserts the
      positive line STILL appears in FULL mode so the first two cannot be satisfied by never stamping
      anything.
- [x] `--lite` audited: `run_lite` always exits before `apply_schemas_preflight`, so no schemas line was
      ever reachable there. The lite case now additionally asserts `! grep '^schemas: '` so a future
      call-site move cannot start asserting readability in a mode that never checked.
- [x] Audited the rest of this change's SUMMARY output for the same class: the only other lines it adds
      (`missing-schemas:` ×2 variants, `preflight: FAIL (…)`, `hint: expected …`) are emitted ONLY on the
      strict path, after the check actually ran and failed, and every value in them is derived from that
      performed check. Nothing else asserts an unverified fact.

## 9. Review round 2 corrections (rust-reviewer: no blockers; 6 nits + roborev job 9's 2 findings)
- [x] Rebased onto `origin/main` `8e85b9e` — roborev job 9 was VOID for certification (`origin/main`
      advanced 4 commits mid-review, so `sha-assert` FAILed against a stale base). No conflicts.
- [x] **NIT-A** — the AC-(b) "no relative path labelled absolute" case was VACUOUS: it matched
      `--preflight-schemas` STDOUT, but `expected absolute path:` is `apply_schemas_preflight` STDERR,
      so it passed unconditionally — including after a full revert of the fix. Re-pointed at the real
      FULL-gate emit (stdout+stderr capture + the summary file) and PROVEN discriminating by reverting
      the reject branch and watching it fail.
- [x] **NIT-B** — the N4 marker rule had no discriminating control (in a healthy checkout the retired
      fixtures-keyed walk returns the same path). Factored `workspace_root_from(start)` and added a
      synthetic `outer/{Cargo.toml,test-data/schemas}` + `outer/inner/{Cargo.toml,cqlite-core}` layout
      where the two rules DISAGREE; proven by reverting the rule and watching it fail.
- [x] **NIT-C** — named the in-repo counterexample to "nearest `[workspace]` is always the enclosing
      checkout": `fuzz/Cargo.toml` declares its own (#1614). Benign (loud failure, never a wrong-tree
      borrow); recorded in both mirrors' comments.
- [x] **NIT-D** — corrected the `--preflight-schemas-line` comment: it can NEVER observe a FAIL SUMMARY
      because `emit_summary`/`_tree_meta_array` are defined after the arg dispatch. The two
      `command not found` lines are gone: the hook stubs both so the strict-failure path exits non-zero
      with one named token.
- [x] **NIT-E** — de-staled this file: the deleted `check_schema_files` no longer listed as delivered,
      the retired fixtures-keyed walk no longer described as the design, case counts corrected.
- [x] **NIT-F** — narrowed the "named non-check" requirement to a lenient mode **that reached the
      preflight**, so it stops contradicting its own `--lite` scenario (and `--only fmt`, which skips the
      dataset preflight entirely).
- [x] **B1 prose** — replaced the unearned "agree by construction" in `fixture_roots.rs` and `spec.md`
      with what is actually true: two hand-written mirrors, EQUIVALENT and PINNED BY SELF-TESTS, walked
      case by case over the whole input table.
- [x] **roborev job 9 finding 1** — single-sourced override presence (`_gate_schemas_override`): the
      reject helper trimmed before deciding presence while the root/source helpers tested the raw `-n`
      value, so a directory literally named `"   "` would have been reported as the override while Rust
      treated the var as unset. Pinned with a whitespace-named-directory case in a synthetic checkout.
- [x] **roborev job 9 finding 2** — `find -H`: a `DATASET_ROOT` that is ITSELF a symlink was never
      descended, so every count came back 0. The reported symptom understated it — verification FAILED
      outright, so `--verify-only` called a good corpus unusable on exactly the symlinked layout #3148
      documents. Fixed and pinned with a symlinked-root case.
