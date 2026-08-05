# Proposal: Narrow `classify-docs-only.sh`'s `docs/` prefix so executables under `docs/` cannot skip the correctness gate (issue #3250)

**Milestone:** maintenance (CI merge-gating semantics / agent-team automation) · **Priority:** P1 ·
**Routing:** design-driven — this decides **which PRs are allowed to bypass the correctness gate**, both
error directions carry real cost (under-narrow ⇒ the hole stays open; over-narrow ⇒ every report PR pays a
~14-minute gate), it must consume a sibling subsystem's declaration without re-drifting from it, and it
amends doctrine (#3042 CITE-AND-WAIVE). No external oracle exists; the fix is a gate contract plus
doctrine. · **Issue:** #3250 ·
**Related:** **#3229** (the sibling defect on the *reviewer* side, merged — it declares the artifact
constants this change imports), #2645 / epic #2636 (introduced the docs-only short-circuit), #2910 (the
`required` tier aggregator — measured NOT to mitigate this), #3042 (the CITE-AND-WAIVE doctrine this
narrows), #3283 (the deferred exclusion-set guard), #3278 (roborev's compiled-in deny-list), #3217 /
PR #3222, #3026 / PR #3081, #3100 / PR #3216 (the three merged, never-gated harness PRs).

## Why

`scripts/ci/classify-docs-only.sh:46` returns docs-only for **any** path under `docs/` on the strength of
the path prefix alone:

```bash
case "$path" in
  docs/*) return 0 ;;      # extension-blind; bash `case` `*` crosses `/`
```

Its **only** consumer is `.github/workflows/pr-gate.yml:95`, and **every** correctness step in
`pr-gate-core` is guarded by `if: steps.classify.outputs.docs_only != 'true'` — workflow-policy
validation, dataset-pin agreement, release-version agreement, the Flight Dockerfile Rust-pin lockstep,
Rust setup, `Cargo.lock` freshness, `cargo fmt`, `cqlite-core` clippy `-D warnings`, the all-feature
build, the fast lib tests, and the #2644 query-semantics oracle. Skipping them does not skip the *check*:
`pr-gate-core` still concludes `success`, and `required` — the sole branch-protection context — is
satisfied. So a PR whose whole diff is under `docs/` reports green having compiled and tested nothing.

That is correct for prose. It is **wrong for code**, and this repository ships code under `docs/` **by
convention**: WS0-style measurement harnesses live in `docs/reports/*-artifacts/`, a convention the owner
has ruled stays. Currently tracked under `docs/`: 43 `.sh`, 27 `.py`, 4 `.yml`, 4 `.c`, 3 `.yaml`,
3 `.bt`, 2 `.cql`, 1 `.toml`, 1 `.rs`, and **3 extensionless mode-100755 harness binaries** — plus a
compilable Cargo crate (`docs/reports/ws0-3026-artifacts/ws0-cqlite/scan-harness/` with `Cargo.toml` +
`src/main.rs`). The classifier calls every one of them documentation.

**It has already happened three times.** `pr-gate-core` concluded SUCCESS in **16 s** (PR #3222, 188
files, 0 outside `docs/`, 34 executables), **14 s** (PR #3081) and **13 s** (PR #3216), against a
~14-minute baseline for a real code PR (PR #3239: 13m 56s; PR #3236: 14m 20s).

**#2910's tier aggregation does not mitigate it** — measured, not assumed. The registry holds exactly one
tier (`flight`), and `flight-ci.yml:148` leaves `docs/**` *deliberately unmandated*, so the tier emits its
explicit not-applicable success and `required` is satisfied. Every other PR-triggered workflow is
`exempt:`. This change starts from **no safety net**.

**And the failure mode is not only mechanical.** CLAUDE.md's #3042 CITE-AND-WAIVE rule tells an agent that
a "docs-only" diff "cannot change the compiled binary", qualified by "no `src`, no `Cargo.*`". A #3222-shaped
diff contains **both** — under `docs/reports/ws0-3026-artifacts/ws0-cqlite/scan-harness/`. So an agent
correctly following documented doctrine reaches the same wrong conclusion and waives a real red as
"pre-existing by definition". Fixing the classifier without fixing the doctrine leaves the human failure
path intact.

## What Changes

1. **The `docs/` prefix stops being a verdict (AC1).** `is_docs_file()` gains no new `case` arm; instead
   the blanket `docs/*) return 0` is replaced by a **single dispatch** to one new function that is the
   *only* place a path under `docs/` is classified. It returns docs-only ONLY on an **affirmative** match
   against a named allowlist layer, and `full` for everything else — including every extensionless path
   and every unrecognized extension under `docs/`. `.sh`/`.py`/`.bt`/`.c`/`.rs`/`.toml`/`.cql`/`.yml`/
   `.yaml` under `docs/` therefore force the full gate, as does a docs-hosted Cargo crate.
2. **#3229's artifact declaration is IMPORTED, never restated (AC5).** The classifier sources
   `scripts/flow/roborev-review-oracles.sh` and consumes `CODE_FREE_ARTIFACT_EXTENSIONS` and
   `CODE_FREE_ARTIFACT_DIR_GLOBS` (plus its component-wise directory matcher
   `roborev_path_in_artifact_dir`) from the single declaration that file already advertises as
   *"THE SINGLE DECLARATION: it is imported, never redeclared (`scripts/ci/classify-docs-only.sh` will
   import it — issue #3250)"*. Direct import is chosen over AC5's drift-check substitute because it is
   **measurably expressible** (evidence in `design.md` D2). A missing, unsourceable, or empty declaration
   is a **fail-closed `full`**, never a silent fallback.
3. **Code-bearing artifact formats are scoped by DIRECTORY, inert dumps are not.** This is where the gate's
   question and #3229's question legitimately diverge, and the divergence is derived from CLAUDE.md's own
   asymmetry rule rather than invented: an inert dump (`.txt`, `.log`, `.jsonl`, …) is docs-only anywhere
   under `docs/`, while a **code-bearing** format (`.json`, `.html`) is docs-only only INSIDE an
   artifact-bearing directory — because such a file can be functional configuration under any path. The two
   live proofs are `docs/observability/grafana/dashboards/cqlite-overview.json` (guarded by the full gate's
   own `kit-dashboard-drift` component) and `docs/reports/delivery-telemetry.schema.json`. Measured cost of
   the scoping: of 808 tracked artifact-extension files under `docs/`, **803 are inside the four artifact
   directories and stay fast; 5 are outside**, and the design assigns each of the 5 deliberately.
4. **One canonical decision point, pinned by a mutation-tested structural assert.** #3229 spent three
   review rounds patching one path-normalisation consumer at a time; the lesson is applied here up front.
   The self-test asserts structurally that no second `docs/`-deciding site exists, that the classifier
   holds **no literal copy** of the imported lists, and — by mutating the *real* declaration in a temp tree
   — that the classifier's verdict actually follows it. A symmetric mirror of the constant would be
   invariant to a shared defect (#3042's blindness in shell) and is therefore forbidden.
5. **A closed verdict grammar for the imported set.** Every extension in
   `CODE_FREE_ARTIFACT_EXTENSIONS` is assigned to exactly one bucket (inert / code-bearing /
   answered-by-the-image-layer). An upstream extension assigned to none FAILs the self-test with a
   **greppable reason** — AC5's "mechanical check that FAILs when the two lists disagree", applied where
   drift can actually occur now that the list itself is imported.
6. **The changed-file list is fed to the classifier RAW.** `pr-gate.yml` computes it with
   `git -c core.quotePath=false diff --name-only`, and the classifier **fail-closes any path spelling it
   cannot read as a raw repo-relative path** (a C-quoted spelling, i.e. one beginning with `"`). This
   repository tracks 40 space-bearing and several non-ASCII paths under `docs/`; classifying
   `"docs/\303\251.md"` on the extension `md"` is exactly the normalisation defect that cost #3229 six
   blockers. Both directions are specified: unreadable ⇒ `full`, and legitimate non-ASCII prose is
   classified on its real extension.
7. **Regression tests in the existing style (AC3).** `scripts/tests/test_classify_docs_only.sh` keeps its
   23 asserts and its Ruby workflow-contract assertion and gains additive cases: `.sh`/`.py`/`.bt` under
   `docs/reports/*-artifacts/`; the docs-hosted `Cargo.toml` + `src/main.rs`; ONE executable hidden among
   many prose files in both orders (the real #3222 shape); the extensionless 100755 harness binaries; the
   two functional-config `.json` files; and negatives proving prose + inert artifacts still short-circuit.
8. **Recorded demonstration on the real shapes (AC4).** The **real 188-path PR #3222 file list** (from
   `gh api repos/pmcfadin/cqlite/pulls/3222/files`) replayed through the new classifier must yield `full`,
   and a prose-only WS0 diff must yield `docs-only`. Recorded as evidence in the PR and a committed
   artifact — not asserted as a test, since the input is a historical API response.
9. **Doctrine in the same change (AC6).** CLAUDE.md's #3042 CITE-AND-WAIVE paragraph is narrowed so it no
   longer implies everything under `docs/` is non-compiled input: the `docs/reports/*-artifacts/`
   convention is named explicitly, the waiver is scoped to a **genuinely prose** diff, and the classifier
   is named as the mechanical test of that. The matching published page
   (`website/src/content/docs/agents-developing/gate-contract.md`, which today carries the gate contract
   but **no** CITE-AND-WAIVE text at all) gains the rule, cross-referenced from `roborev-findings.md`'s
   existing code-free-census definition so the two doctrines cannot drift. Publication is accepted by
   **grepping the served page for a distinctive new phrase**, never by HTTP 200.
10. **The backfill ruling is recorded (AC7).** The decision is the owner's; the change records it with its
    reason. `design.md` D7 states the recommendation and the evidence that bounds the exposure — the
    docs-hosted `scan-harness` crate is **not a workspace member** (verified via `cargo metadata`), so no
    `pr-gate-core` step reads any path in those three diffs.

## Non-goals

- **Not relocating the measurement harnesses out of `docs/reports/*-artifacts/`.** Owner-ruled: the
  convention stays. No move is proposed and none is smuggled in as tidying.
- **Not touching `.roborev.toml`, #3229's wrapper, its census, or its vacuity/SHA traps.** This change is a
  pure *consumer* of #3229's declaration. It adds no pattern, removes none, and changes no roborev behaviour.
- **Not adding `paths:`/`paths-ignore:` to `pr-gate.yml`'s trigger.** A filtered required check never
  reports and would block every affected PR forever (`pr-gate.yml:71-78`). The always-emit classifier
  design is correct and is not what is being fixed.
- **Not changing the `required` tier registry or promoting tiers (#2910).** That mechanism works as
  designed; it simply does not cover this hole.
- **Not widening the gate for prose PRs.** A `.md`/image/`LICENSE`/report-artifact PR keeps its
  seconds-long green (AC2). Measured: the change moves **0** tracked prose or image file to the full path.
- **Not changing the classifier's own image/legal allowlist.** `.png`/`.jpg`/`.jpeg`/`.gif`/`.svg`/
  `.webp`/`.ico` and `LICENSE*`/`NOTICE*` keep their existing repo-wide behaviour; the divergence from
  roborev's directory-scoped `.svg` is legitimate and is recorded rather than silently harmonised
  (`design.md` D3c).
- **Not predicting roborev's exclusion set** (#3283) or modelling its compiled-in deny-list (#3278). This
  change consumes a declaration; it models nothing.
- **No Rust code, no library surface, no on-disk format work.** Nothing touches `cqlite-core`, the
  bindings, the CLI, the no-heuristics decode path, or the <128MB memory budget.

## Impact

- **Gate script:** `scripts/ci/classify-docs-only.sh` — the blanket `docs/*` arm replaced by one dispatch
  to a single new classifier function; a fail-closed import of #3229's declaration; a raw-path guard.
- **Workflow:** `.github/workflows/pr-gate.yml` — the `Classify docs-only diff` step computes the
  changed-file list with `-c core.quotePath=false`. No trigger change, no step-gating change, no
  `required` change.
- **Tests:** `scripts/tests/test_classify_docs_only.sh` — additive cases plus the structural/mutation
  asserts. It runs in the full gate's `tooling-tests` component (`scripts/agent-gate.sh:5519`). Additive
  by construction, so it rebases cleanly over #3296's concurrent edits to the *sibling* suite
  (`test_roborev_review_guard.sh` — a different file).
- **Doctrine surfaces:** `CLAUDE.md` (#3042 CITE-AND-WAIVE paragraph),
  `website/src/content/docs/agents-developing/gate-contract.md`, and a cross-reference in
  `website/src/content/docs/agents-developing/roborev-findings.md`.
- **Cross-subsystem coupling, stated so it is not rediscovered:** `scripts/ci/` now depends on a
  declaration in `scripts/flow/`. The declaration's *content* is pinned to **roborev v0.61.2** because it
  mirrors `.roborev.toml`, whose pattern semantics were recovered from that binary. The
  **re-verify-on-upgrade obligation is inherited** — and its scope is stated precisely in `design.md` D4:
  a roborev upgrade can move the declaration, and the declaration now moves the **gate**.
- **CI cost:** unchanged for prose PRs (0 tracked prose/image files newly forced to `full`). A PR whose
  whole diff is one of the 5 measured outside-glob artifact files pays the ~14-minute core; 3 of the 5 are
  correctness-bearing and *should*, and the 2 inert ones are deliberately kept fast (D3b).
- **No-heuristics mandate (#28):** unaffected — that mandate governs on-disk type/format inference in the
  SSTable read path. Nothing here infers anything; the classification is an allowlist over committed path
  names, and the exec-bit heuristic roborev uses is deliberately **not** adopted (D3d).
- **Public binding surfaces (Python/Node/CLI), memory budget:** untouched.
