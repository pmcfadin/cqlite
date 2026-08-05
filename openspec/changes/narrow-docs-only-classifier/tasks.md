# Tasks: narrow-docs-only-classifier (issue #3250)

> Design decided in `design.md`. In one line: replace `classify-docs-only.sh`'s blanket `docs/*) return 0`
> with a SINGLE dispatch to one classifier function that returns documentation only on an affirmative
> allowlist match (prose/image/legal · inert imported artifact anywhere under `docs/` · code-bearing
> imported artifact inside an artifact-bearing directory), fail-closed on everything else including all
> extensionless paths; **import** #3229's `CODE_FREE_ARTIFACT_EXTENSIONS` /
> `CODE_FREE_ARTIFACT_DIR_GLOBS` / `roborev_path_in_artifact_dir` rather than restating them; pin the
> boundary with mutation-tested structural asserts; narrow the #3042 CITE-AND-WAIVE doctrine in the same
> change. AC→requirement map is at the top of `specs/docs-only-gate-classification/spec.md`.
>
> **AC7 is RULED (owner, 2026-08-04, Seam 1): accept as-is, no retroactive core-gate run** — one ruling
> covering #3229 AC7 and #3250 AC7, with the promotion condition. Task 8 is therefore the RECORDING only;
> the ruling as given, its date, reason, bounding evidence and condition of change are in `design.md`
> D7.RULING. Do not re-ask it.

## 1. Import #3229's artifact declaration, fail-closed (surface: `scripts/ci/classify-docs-only.sh`)
- [ ] Source `scripts/flow/roborev-review-oracles.sh` resolved **relative to the classifier's own
      location** (`${BASH_SOURCE[0]}`), never `$PWD`. Location-relative resolution is what makes task 5's
      mutation test possible.
- [ ] After sourcing, verify the declaration is usable: file present, source succeeded,
      `CODE_FREE_ARTIFACT_EXTENSIONS` non-empty, `CODE_FREE_ARTIFACT_DIR_GLOBS` has ≥1 element. On any
      failure, classify **every** path under `docs/` as `full` and print a named reason. Never degrade to
      "prose-only allowlist and carry on" — an infra fault must not loosen the gate.
- [ ] Record the coupling at the import site: this classifier now depends on the declaration's CONTENT
      (not on roborev v0.61.2's `FormatExcludeArgs` pathspec semantics — it feeds roborev no pathspec and
      reads no `.roborev.toml`), and an edit to the declaration now moves the CORRECTNESS GATE as well as
      the reviewer. State the re-verify-on-upgrade obligation in those terms (`design.md` D4).
- [ ] Do NOT copy the extension list or any directory glob as a literal, and do NOT reimplement the
      directory matcher — `roborev_path_in_artifact_dir`'s component-wise walk is load-bearing (a bash
      `case` glob crosses `/`; git's `:(glob)` `*` does not).

## 2. One canonical decision point (surface: `scripts/ci/classify-docs-only.sh`, `is_docs_file()`)
- [ ] Delete the blanket `docs/*) return 0 ;;` arm. Replace it with a single dispatch to one new function
      that is the ONLY place a path under `docs/` is classified. Keep the sensitive-directory escape
      (`.github/`, `scripts/`, `test-data/`) and the global `*.md`/image/`LICENSE*` arms unchanged for
      paths outside `docs/`.
- [ ] Implement the three affirmative layers inside that function, disjoint by extension so their order
      cannot change an answer: L1 prose/image/legal at any depth (the classifier's OWN semantics,
      unchanged, including `.jpg`/`.gif`/`.webp`/`.ico` which are NOT in the imported set); L2 the inert
      bucket at any depth under `docs/`; L3 the code-bearing bucket **only** where
      `roborev_path_in_artifact_dir` says the path is inside an artifact directory.
- [ ] Declare the buckets: inert = `txt jsonl log err csv gz pdf jfr mmd tex diff`; code-bearing =
      `json html`; image-layer = `png svg` (assigned to neither behavioural bucket because L1 answers them
      first, and this change does not alter the image layer — `design.md` D3c). At runtime, an imported
      extension in NO bucket classifies `full`.
- [ ] Closed grammar: no `else return 0`, no "not obviously code ⇒ prose". Every documentation verdict
      comes from a positive match.
- [ ] Do NOT consult git's executable bit for extensionless paths (`design.md` D3d): the input carries no
      mode, a mode read would need a repository and two refs, `chmod -x` must not move a program into the
      documentation class, and all 3 tracked extensionless files under `docs/` are 100755 harnesses.

## 3. Raw-path boundary (surfaces: `scripts/ci/classify-docs-only.sh`, `.github/workflows/pr-gate.yml`)
- [ ] Classifier: fail closed (`full`, named reason) on any path spelling that is not a raw repo-relative
      path — in particular one beginning with `"` (git's C-quoted form). One boundary, one place.
- [ ] `pr-gate.yml` `Classify docs-only diff` step: compute the changed-file list with
      `git -c core.quotePath=false diff --name-only "${BASE_SHA}...${HEAD_SHA}"`. Do NOT switch to `-z`
      (that would change the classifier's newline-delimited stdin contract). Change nothing else: no
      trigger filter, no `if:` on the classify step, no change to the `!= 'true'` heavy-step gates, no
      change to `required`.

## 4. Behavioural regression cases (surface: `scripts/tests/test_classify_docs_only.sh`)
- [ ] Additive only — keep all 23 existing asserts and the Ruby `pr-gate.yml` contract block, so the suite
      rebases cleanly over #3296's concurrent edits to the SIBLING suite
      (`test_roborev_review_guard.sh` — a different file). If the two lanes ever touch the same lines,
      raise `coord:needs-attention` rather than racing.
- [ ] `assert_full` cases: `.sh`, `.py`, `.bt` under `docs/reports/*-artifacts/`; the docs-hosted
      `Cargo.toml` and `src/main.rs` (each alone and together); an extensionless harness path
      (`docs/reports/ws0-3026-artifacts/ws0-results/ws0-readbw`); one executable LAST among 20 prose files
      and the same set with it FIRST (order-independence — the real #3222 shape); unrecognized extensions
      under `docs/` (`.rb`, `.jq`, `.mjs`); `docs/observability/grafana/dashboards/cqlite-overview.json`;
      `docs/reports/delivery-telemetry.schema.json`; `docs/reports/a/b-artifacts/x.json` (the `case`-glob
      separator-crossing case); a C-quoted spelling.
- [ ] `assert_docs_only` cases: prose + image + legal; a WS0 report set (`README.md` + `.txt` `.jsonl`
      `.csv` `.log` `.err` `.png` `.json` under an artifact dir); inert extensions OUTSIDE an artifact dir
      (`docs/reports/delivery-telemetry.jsonl`, `docs/sstables-definitive-guide/pandoc-header.tex`,
      `.../statistics-db-annotated-dump.txt`); `docs/img/photo.jpg`, `.gif`, `.ico`;
      `docs/reports/ws0-3217-artifacts/x.json` and `docs/observability/jfr-reports/run.html` and
      `docs/round-artifacts/2026-08/deep/nested/out.json` (nested/`**` matches); a raw non-ASCII `.md`; a
      space-bearing `.md`.
- [ ] Negative-control pair: `docs/reports/ws0-3217-artifacts/README.md` + `.../harness/parse-runqlat.py`
      ⇒ `full`, proving a prose sibling does not rescue the set.

## 5. Structural + mutation asserts (surface: `scripts/tests/test_classify_docs_only.sh`)
- [ ] Assert the classifier source contains **no** literal copy of the imported extension list and **no**
      literal copy of any of the four directory globs.
- [ ] Assert `is_docs_file()` has no `case` arm returning documentation for a `docs/`-prefixed pattern, and
      that no second site in the file decides a `docs/` path's class. **Mutation-test the assert**: a temp
      copy with `docs/*) return 0 ;;` reintroduced must FAIL it, and a temp copy with a second deciding
      function must FAIL it, while the real file passes.
- [ ] Mutation-test the IMPORT: temp tree with a copy of the classifier + a copy of the **real**
      `roborev-review-oracles.sh` whose declaration is mutated (synthetic inert extension added, one
      directory glob removed); assert the classifier's verdicts MOVE accordingly. A classifier with its own
      hardcoded lists fails this.
- [ ] Assert the bucket partition against the **real** declaration (union equals
      `CODE_FREE_ARTIFACT_EXTENSIONS`, buckets pairwise disjoint). A synthetic extension added upstream must
      FAIL with a greppable message naming the extension and `#3250`. Never mirror the list into a fixture —
      a symmetric mirror shares the defect and greens while broken (#3042's blindness in shell).
- [ ] Assert the fail-closed import: temp tree with the declaring file absent, and one with an empty
      declaration ⇒ `full` for a path that is `docs-only` under the real declaration, with a named reason.
- [ ] Confirm the suite stays hermetic (pure shell + local `git` reads + optional Ruby) and still runs in
      the gate's `tooling-tests` component (`scripts/agent-gate.sh`).

## 6. Recorded demonstration on the real shapes (AC4 — evidence, not asserts)
- [ ] Fetch PR #3222's real 188-path list (`gh api repos/pmcfadin/cqlite/pulls/3222/files`, both pages);
      replay through the amended classifier ⇒ expect `full` / exit 1. Record the verdict, exit status, path
      count, offending-path count and first offending path.
- [ ] Replay the same list with the 34 executable/config-as-code paths removed ⇒ expect `docs-only` /
      exit 0. Record it.
- [ ] Repeat the replay for PR #3081 and PR #3216 and record the verdicts.
- [ ] Record the pre-change verdict on the same #3222 input (`docs-only`) so the demonstration shows a
      CHANGED verdict. Put the numbers in the PR body and the change; add no network-dependent gate
      assertion and commit no 188-path fixture.

## 7. Doctrine, in the same change (AC6)
- [ ] `CLAUDE.md`: narrow the #3042 CITE-AND-WAIVE paragraph — name the `docs/reports/*-artifacts/`
      convention, scope the waiver to a genuinely prose diff, name `scripts/ci/classify-docs-only.sh` as
      the mechanical test, name the falsifying case (`scan-harness`'s `Cargo.toml` + `src/main.rs` satisfy
      the old qualifier textually while being false materially). Retain the cited-issue requirement and the
      compiled-input-voids-the-waiver clause.
- [ ] `website/src/content/docs/agents-developing/gate-contract.md`: add the narrowed rule (CITE-AND-WAIVE
      appears nowhere on the site today — verified by grep). Cross-reference it from
      `roborev-findings.md`'s existing code-free-census definition so the review-side and gate-side
      "docs-only" definitions are linked, not independently maintained.
- [ ] Verify publication by **served content**:
      `curl -sS https://pmcfadin.github.io/cqlite/agents-developing/gate-contract/ | grep -c '<new phrase>'`
      must be non-zero. A `0` is not-yet-published — wait and re-check (the CDN has served stale content for
      ~3 minutes after a successful deploy). Never accept HTTP 200 or a green deploy as evidence.

## 8. AC7 — the backfill ruling (OWNER decision; do not decide in-band)
- [ ] Ask the owner, carrying `design.md` D7's recommendation (accept as-is; one ruling covering #3229 AC7
      and #3250 AC7) and its evidence: `scan-harness` is not a workspace member (`cargo metadata
      --no-deps`, 16 packages, none under `docs/`), and no skipped `pr-gate-core` step reads any path in
      those three diffs — so a retroactive run is equivalent to running the core on `main`.
- [ ] Record the ruling, its date, its reason, its bounding evidence and its condition of change (promotion
      of harness code into a shipped path ends the exemption) in `design.md` D7 and the PR body. Silence is
      the only failing outcome.

## 9. Verification, review, gate, merge
- [ ] `bash scripts/tests/test_classify_docs_only.sh` green standalone; `FAIL=0`.
- [ ] `--lite` each fix round, summary-file redirect:
      `AGENT_GATE_SUMMARY_FILE=/tmp/gate-lite-3250.txt bash scripts/agent-gate.sh --lite > lite.log 2>&1
      < /dev/null`, then read only `/tmp/gate-lite-3250.txt`. Poll on `RESULT: (PASS|FAIL)` —
      `INCOMPLETE` is a liveness sentinel, not a verdict — and check `tree-integrity:`.
- [ ] `bash scripts/agent-gate.sh --only tooling-tests` for the fast targeted signal on the suite.
- [ ] Review-first: `rust-reviewer` is N/A (no Rust); run roborev on the lite-green, PUSHED diff via the
      ONLY sanctioned form —
      `bash scripts/flow/roborev-review.sh --agent codex --model gpt-5.6-sol --repo /home/ubuntu/workspace/cqlite-wt/issue-3250 --base origin/main`
      — retaining only the `==== ROBOREV REVIEW SUMMARY ====` block. Any non-PASS terminal `RESULT`
      (including `NOTHING-TO-REVIEW`) is a failed round. This diff carries shell + workflow code, so it is
      NOT code-free and MUST be roborev-certified. Raise `default_max_prompt_size` in
      `~/.roborev/config.toml` and restart the daemon before the closer's pass (#3257); carry
      `prompt-content:` verbatim in the closer's packet; never slice the base.
- [ ] Each roborev blocker: fix → `--lite` re-cert → re-review. Nits batch into ONE linked follow-up issue
      at merge time.
- [ ] Open the PR. Rebase over #3296 if it has merged; confirm the test additions are still additive.
- [ ] Hand the endgame to `flow-closer`: the ONE full gate of record
      (`AGENT_GATE_SUMMARY_FILE=/tmp/gate-3250.txt bash scripts/agent-gate.sh > gate.log 2>&1 < /dev/null`,
      then read only the summary file — never `gate.log`), then **C** (`spec-auditor` against
      `openspec/changes/narrow-docs-only-classifier/specs/**`), then the final roborev pass, then
      `scripts/flow/premerge-assert.sh <pr> <certified-sha>`, then
      `gh pr merge --auto --squash --delete-branch`. Note the CITE-AND-WAIVE waiver does NOT apply to this
      PR's own gate: the diff touches workflow and script inputs, so any failure is presumed ours.
- [ ] `flow-finalize`: archive the change, stamp delivery telemetry via a `telemetry-<N>` worktree PR (never
      a direct push to `main`), remove the worktree/branch, close the issue with a traceable comment.
