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

## 10. Review round 3 (rust-reviewer: mergeable as-is; roborev job 10 VALID, 3 findings)
- [x] **Prose residue** — the "by construction" overclaim survived at `scripts/agent-gate.sh` (the shell
      mirror's OWN header — the worst possible place, since a future editor reading it would believe
      shell/Rust equivalence is structural and edit one side without re-walking the input table) and
      `design.md`. Both now state what is true: two hand-written mirrors, equivalent today, PINNED BY
      `test_agent_gate_schemas_preflight.sh`, with an explicit "if you edit either side, re-walk the table
      and re-run that self-test". Blank separator restored so the sentence no longer dangles above
      `_gate_schemas_override_present`. The genuinely structural uses elsewhere were left alone.
- [x] **job 10 finding 1** (Medium, real) — `mktemp -d` was unchecked under `set -uo pipefail` (no
      errexit, deliberately), so a failure left `$tmp` EMPTY and every derived path became root-level
      (`/ds-corpus`, `/schemas-empty`, …) which a privileged CI job would create, with the EXIT trap then
      running `rm -rf ""`. Now validated (non-empty AND a directory) BEFORE the trap is armed. Pinned by a
      case that stubs a failing `mktemp` on PATH and asserts a loud abort plus the absence of root-level
      paths. That case re-invokes this script, so a bounded CHILD PROBE MODE was added — without it the
      child ran every case including its own and recursed without bound (observed during the revert proof).
- [x] **job 10 finding 2** (Low as filed; treated as the mis-certification class) — command substitution
      STRIPS TRAILING NEWLINES, and the round-2 refactor consumed the override through `$( )`. Measured:
      with `CQLITE_SCHEMAS_ROOT=$'<real dir>\n'` the gate reported `STATUS: OK` / `SOURCE: override` /
      `ROOT: <real dir>` while Rust kept the newline, got `is_dir() == false`, and degraded to the
      checkout — gate certifying the root the run did not use. Note this was a regression INTRODUCED by
      the round-2 fix for finding 9-1 (pre-refactor the shell read the raw var and agreed with Rust).
      Closed twice over: presence is now a STATUS-returning predicate with the value read directly from
      the environment (no substitution anywhere on the value path), AND control-character values are
      rejected fail-closed on both sides.
- [x] **job 10 finding 3** (Low, real) — the "absolute but unusable" test hard-coded
      `/nonexistent-cqlite-schemas-3148`: not absolute under Windows path semantics and not guaranteed
      absent on Unix, so if someone created it the test would silently assert the OPPOSITE branch. Now
      built under a fresh `TempDir` with native path handling, asserting both `is_absolute()` and
      `!exists()` so absence is a property of the construction. Test-hygiene, not a behavior fix — no
      revert proof applies.
- [x] Revert proofs performed for findings 1 and 2 (both cases FAIL on a revert, pass restored). Controls
      re-run because finding 2 changed resolution logic on both sides: hostile-layout 8/8 + 3/3 + 1/1 +
      1/1, empty-override 4-pass/4-fail, relative-override 4 fail-closed, negative-control FULL gate rc=1
      with the marker. Self-test 30 → 32 cases; contract tests 8 → 9.

## 11. Gate of record FAIL + C (spec-auditor) PARTIAL — round 4
- [x] **`tooling-tests: FAIL` root cause** — NOT nesting (the leading hypothesis is REFUTED: the schemas
      self-test passed 32/32 INSIDE the gate, `tooling-tests.log:481`). The failure was the PRE-EXISTING
      `test_agent_gate_tree_portability.sh` derived-inventory UNIQUENESS assert (`n=45 uniq=44`), because
      round 3's NIT-D fix STUBBED `emit_summary`/`_tree_meta_array` in the
      `--preflight-schemas-line` hook — a SECOND definition of a `_tree*` function. It passed standalone
      because that portability test only runs inside `tooling-tests`, which had not been run. Replaced the
      stubs with `_SCHEMAS_PREFLIGHT_REPORT_ONLY` (the two failure branches return with the marker in
      `SCHEMAS_LINE` instead of emitting + exiting); a flag on the terminal ACTION carries no decision and
      stamps no new text. Portability test 28/1 → 29/0.
- [x] **C (i) requirement 5 UNCOVERED** — the unreadable-fixture message was asserted by nothing (revert
      `schema_path` to a bare `expect` and every test stayed green). Factored `resolve_schema_path(root,
      source, file) -> Result<_, String>` and added two tests: the message must name the absolute path,
      the root, HOW the root was chosen, the committed-source note and the remedy; and `schema_path`
      itself must PANIC with it (`catch_unwind`, no env mutation). REVERT-PROOF: replacing the body with
      `Err("Path does not exist: …")` FAILs both.
- [x] **C (ii) AC (b) partial — a FALSE message** — the reject branch hard-coded the RELATIVE explanation
      and marker for EVERY rejection, so a control-character value was reported as "relative". Added
      `_gate_schemas_override_reject_kind` and derived prose + marker from it. Pinned at the REAL FULL-gate
      emit (`3148-cc-emit-wording`), asserting the control-character wording is present AND the false
      "relative" wording is absent, in both the block and stderr.
- [x] **C (iii) requirement 12 partial** — nothing exercised the warm-cache `guarantee_usable_root` call
      site or the "NOT the checkout default" NOTE. Two cases now drive the REAL fetch script down the
      warm-skip path (`.dataset-pin` derived from the TRACKED `dataset-pin.env`, never hard-coded), with a
      failing `curl` stub first on PATH so a future pin mismatch dies without network and without reaching
      `rm -rf`.
- [x] **C (iv) requirement 6 partial** — the guard grepped for the function NAME. Factored
      `resolve_datasets_root` / `resolve_datasets_root_if_present` and asserted both shapes behaviourally.
      REVERT-PROOF: giving the fallible shape a checkout fallback FAILs
      `the_two_datasets_root_shapes_differ_as_documented`.
- [x] **C (v)** — `3148-no-optout`: `AGENT_GATE_ALLOW_MISSING_FIXTURES=1` must not buy a pass on a
      schemas-less root; and **#3192 is now cited by number** in `proposal.md` and `spec.md`, together with
      why the reintroduction guard cannot be widened before that migration lands.
- [x] Self-test 32 → 36 cases; contract tests 9 → 12; portability 29/29.

## 12. C re-audit round 5 — requirement 8 defeated by the round-4 fix
> **Process note, recorded deliberately.** This is the **sixth** instance in this delivery of a fix
> introducing a defect, and the **third** time the introduced defect was the very property the change
> exists to guarantee: (1) a positive `schemas:` line asserting an unperformed check (round 2), (2) a
> `$( )`-stripped override diverging the two mirrors (round 3, from round 2's own fix), (3) this — an
> uninitialized, env-readable report-only flag turning the fail-closed `exit 1` into `return 1` (round 4,
> from round 3's comment fix). Each was found by review, not by the tests written alongside the fix. The
> pattern is the argument for the revert-and-watch-it-fail discipline used throughout, and for treating
> "does this fix reintroduce the class one layer over?" as a required question, not a courtesy.
- [x] **The defect** — `_SCHEMAS_PREFLIGHT_REPORT_ONLY` was read as `${…:-}` and never initialized
      (unlike `SCHEMAS_LINE=""`), so an inherited/exported value made the FULL gate's fail-closed branch
      `return 1` at a bare call site with no `errexit`; the run continued and the
      `missing-schemas: FAIL-CLOSED` text could be stamped inside a block reading `RESULT: PASS`.
      Requirement 8 ("no environment opt-out may permit a run to certify with the schemas root
      unreachable") was violated by construction.
- [x] **Mechanism chosen: a POSITIONAL ARGUMENT, not a variable.** `apply_schemas_preflight
      [report-only]`; the hook passes `report-only`, the real gate passes nothing, so STRICT is the
      default that needs no state to be correct. Initialization alone would have closed only the
      INHERITED path — an `export` performed after initialization still wins, because the read happens
      later. `$1` inside a function comes from the CALL; no env var, `export`, or `env -i` can supply it.
- [x] **Pinned** by `3148-no-env-report-only`: a FULL gate over the schemas-less root with
      `_SCHEMAS_PREFLIGHT_REPORT_ONLY=1` — plus three other plausible spellings, because the property is
      that the ENVIRONMENT cannot reach the mode, not that one name was retired — must exit non-zero with
      `missing-schemas: FAIL-CLOSED`, `RESULT: FAIL`, and never `RESULT: PASS`. REVERT-PROOF: restoring
      the uninitialized env-readable flag makes it FAIL (rc=124 — the run sailed past the preflight into
      the components until the case's own `timeout` fired, exactly the "run proceeds" symptom).
- [x] **Class audit** of every variable this change introduced in `scripts/agent-gate.sh`:
      `CANONICAL_SCHEMA_FILES` (unconditional assignment — an inherited value is overwritten; SAFE),
      `SCHEMAS_LINE` (initialized `""`, display-only, carries no decision; SAFE), the hook temporaries
      `_ps_st`/`_ps_rj` (assigned by command substitution before every read; SAFE), every function
      temporary — `mode`, `report_only`, `root`, `missing`, `reject`, `kind`, `why`, `marker`, `n`,
      `_mode`, `d`, `v`, `f`, `out` — declared `local` (unreachable from the environment; SAFE). Env
      values read on purpose: `CQLITE_SCHEMAS_ROOT` (the documented override, now fail-closed on
      relative/control-char), `AGENT_GATE_ALLOW_MISSING_FIXTURES` (pre-existing; pinned as NOT applying
      to schemas by `3148-no-optout`), `ONLY`/`LITE` (gate-internal, initialized at `:1832`/`:1834`).
      `_SCHEMAS_PREFLIGHT_REPORT_ONLY` no longer exists outside one explanatory comment.
- [x] Self-test 36 → 37 cases.

## 13. Final roborev (job 11) — the THIRD instance of the certify-A-use-B class
- [x] **BLOCKER: non-UTF-8 override degraded silently.** `env_dir` used `std::env::var` with a
      catch-all `_ => None`, so `Err(NotUnicode)` collapsed to "unset" and `resolve_schemas_root`
      returned `Ok(checkout)` — while Bash, which handles the value as BYTES, validated the same path
      as a legitimate override. MEASURED before the fix: `STATUS: OK` + `SOURCE: CQLITE_SCHEMAS_ROOT
      override` for a real `bad\xff\xfedir` directory, against Rust's `Err(NotUnicode)`. Same
      certify-A-use-B split already pinned for control-character and relative values; pinning two of
      three is an incomplete fail-closed posture in the mechanism the gate now trusts.
      * **Rust**: `env_os` (`var_os`, no catch-all over an error variant) and
        `resolve_schemas_root(Option<&OsStr>)` — the signature now makes the non-UTF-8 case
        REPRESENTABLE, so it gets an explicit `Err` arm instead of inheriting a lossy conversion.
      * **Bash**: `_gate_schemas_override_is_utf8` → a new `non-utf8` reject kind with its own prose
        and marker. Pure ASCII is accepted with no external tool (valid UTF-8 by definition); anything
        else is validated with `iconv -f UTF-8 -t UTF-8`, and an ABSENT `iconv` REJECTS rather than
        assumes — "could not check" must not mean "accept", or the hole returns on a box without it.
      * Both sides now agree across the whole table: unset → checkout; empty/whitespace → checkout;
        **non-UTF-8 → REJECT**; control-char → REJECT; relative → REJECT; absolute-non-dir → checkout;
        absolute-dir → override. A legitimate MULTIBYTE UTF-8 root is still accepted (asserted), so the
        guard is not an over-broad ban on non-ASCII paths.
- [x] **Datasets asymmetry: real, and hardened here rather than deferred.** The hazard is the same and
      arguably worse — the gate counts `Data.db` under `$CQLITE_DATASETS_ROOT`, so a non-UTF-8 value
      would have had the gate certify a corpus while every test read the checkout's `datasets`, which
      holds only committed byte-parity references and NO `test_basic`: a #2078 vacuous pass with the
      preflight vouching for a corpus the run never used. `datasets_root()` is infallible, so the
      mechanism chosen is **honor the OS value** (`PathBuf::from(&OsStr)`) rather than an actionable
      panic: it is three lines, adds no new failure mode, removes a silent fallback, and makes Rust
      agree with what Bash already does. No follow-up issue needed.
- [x] **NIT 2 (Medium): the printed remedy was CWD-relative.** `git restore …` fails when pasted from
      cargo's package-dir CWD — the same CWD asymmetry this module rejects relative overrides for. Now
      `git -C <workspace_root> …`, with the emitted remedy asserted to carry an ABSOLUTE `-C`.
- [x] **NIT 3 (Low): the fetch remedy interpolated `PIN_FILE`/`DATASET_ROOT` unquoted.** Now `%q`,
      asserted by the same `eval` round-trip used for the export line, on a path with a space and `&`.
- [x] **Three revert-proofs, all three drift instances now covered** (was two of three): reverting the
      Bash UTF-8 guard FAILs `3148-non-utf8-override` (rc=124 — the run sailed past the preflight);
      reverting the Rust arm FAILs `non_utf8_override_is_rejected_fail_closed`; reverting nit 2 FAILs
      `unreadable_fixture_message_names_path_root_source_and_remedy`; reverting nit 3 FAILs
      `3131-remedy-quoting`. Self-test 37 → 40 cases; contract tests 12 → 14.
