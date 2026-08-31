# Tasks: certified-tree-vs-merged-tree (issue #3650, SLICE 1 — base-staleness advisory)

Surface exercised by each task is named, per `openspec/config.yaml` rules.

## 1. The advisory script

- [x] 1.1 Create `scripts/flow/base-staleness.sh` (executable, `set -uo pipefail`).
      **Surface:** the command itself — usable by hand for the standing triage rule
      ("is the fix for this red already on main and merely absent from my base?").
      - Resolve the merge-base (`git merge-base origin/main HEAD`), NOT `origin/main`'s tip (D4/#3392).
      - `N` = `git rev-list --count <merge-base>..origin/main`.
      - Diff paths = `git -c diff.renames=false -c diff.relative=false diff --name-only -z
        <merge-base>...HEAD` (**`-z`**, per #3229's path-normalisation invariant — no path-reading
        `git diff` without `-z`; and both configs pinned OFF, per 1.5 below).
      - `M` = commits in that range touching (diff paths ∪ gate-global set).
      - Print the merge-base, the `origin/main` sha AND its commit date (D5).
      - Never fetch; never write; never mutate a ref.
- [x] 1.2 Hard-code the gate-global list in ONE NAMED place (`GATE_GLOBAL_PATTERNS`), no env override
      (D1/D1a/#3312), with a comment stating the MEMBERSHIP PREDICATE and how to add an entry.
- [x] 1.3 Output ANCHORING (D2 as revised — the absolute substring form was FALSIFIED BY REVIEW):
      `BASE-STALENESS: ` on **every** line of stdout AND stderr; every dynamic field
      control-character SANITIZED; the verdict only on a `verdict ` line carrying a token from the
      closed set `STALE-RECOGNISED` / `NO-STALENESS-RECOGNISED` / `UNMEASURED` (prose on
      `verdict-detail`); zero prints `0 RECOGNISED`; `NON-EXHAUSTIVE` lines on every run declaring
      BOTH gaps. The script's own STATIC TEMPLATE TEXT carries no `PASS`/`OK`/`RESULT:` — a
      structural property of the source, not a claim about a run.
- [x] 1.5 Add `scripts/tests/**` to the gate-global set (D1b, measured: 28 → 37 of 107); declare the
      list NON-CLOSED in the output (D1c); pin `diff.renames`/`diff.relative` OFF on the porcelain
      call so the two path sources are rename-symmetric and root-relative (D1d — unpinned this is a
      FAIL-OPEN on any PR that renames a path).
- [x] 1.4 Exit codes (D3): `0` no-staleness, `4` stale, `5` unmeasured, `3` usage. State the
      **`UNMEASURED` MUST be treated as stale** consumer contract in the header.

## 2. Tests — `scripts/tests/test_base_staleness.sh`

- [x] 2.1 Harness modelled on `scripts/tests/test_premerge_assert.sh`: `ok()`/`bad()` counters, one
      `mktemp -d` with a cleanup trap, `# --- Case N: <claim> ---` banners, a `=== base-staleness: N passed,
      M failed ===` tail that exits non-zero on any failure.
      **Surface:** the script's CLI, driven against synthetic git repos built in the temp dir.
- [x] 2.2 Case: stale base with blast-radius churn → `STALE-RECOGNISED`, exit 4.
- [x] 2.3 Case: up-to-date base → `behind 0 commits`, `NO-STALENESS-RECOGNISED`, exit 0.
- [x] 2.4 Case: merge-base is used, not the base ref's tip (branch whose main advanced past its point).
- [x] 2.5 **Case (motivating, pinned):** diff sharing NO path with a commit behind that touches
      `.config/nextest.toml` → `STALE-RECOGNISED`. This is PR #3362's shape and the case the narrow
      definition fails.
- [x] 2.6 Case: unrelated churn only → counted in `N`, not in `M`, verdict `NO-STALENESS-RECOGNISED`.
- [x] 2.7 Cases (AC5 as revised — ANCHORED, evaluated after the LAST case over the accumulated output
      of every case; the predecessor ran MID-SUITE and inspected Cases 2-6 only):
      every nonempty output line of every case, stdout and stderr, carries the `BASE-STALENESS: `
      prefix; every `verdict ` token is from the closed set; every measurement run emits exactly one
      `verdict ` line; a structural assert over the script SOURCE for the static-template property;
      fixtures whose MATCHED paths contain `OK`, `PASS`, a space and a NEWLINE, printed verbatim
      (newline escaped visibly); and a planted mutant reducing the sanitizer to a pass-through, which
      must break the anchor.
- [x] 2.14 Case (DERIVED, per gate-global ENTRY): read `GATE_GLOBAL_PATTERNS` at run time, synthesize
      one probe commit per entry per recognised shape, assert `STALE-RECOGNISED` **per entry** (never a
      suite-wide `ran > 0`, #3220), FAIL CLOSED on an empty derivation. Reconcile against the
      INDEPENDENT committed list in `design.md` D1a and probe the UNION, because a single derivation
      cannot pin an entry — dropping one would drop its own probe (oracle sharing a source with its
      subject). Verified by mutation sweep: each of the 10 entries reds when dropped.
- [x] 2.13 Cases (D1d): a PR that RENAMES a path plus a commit behind editing the OLD path is
      `STALE-RECOGNISED`, and reds against a copy with the porcelain pin removed; and
      `diff.relative=true` with cwd in a subdirectory still stales.
- [x] 2.8 Case (AC5): a zero blast radius prints `0 RECOGNISED`, never a bare `0`; `NON-EXHAUSTIVE` present.
- [x] 2.9 Case: missing `origin/main` → `UNMEASURED`, exit 5, and output contains neither
      `NO-STALENESS-RECOGNISED` nor a bare blast-radius `0`.
- [x] 2.10 Case: no merge-base → `UNMEASURED`, exit 5.
- [x] 2.11 **Planted-mutant case** (D8, AC6) following `scripts/tests/test_ws0_perf_invocation_lint.sh:812-830`:
      copy the script, empty the gate-global set, assert case 2.5 reds against the copy — and assert the
      planted defect is genuinely the one described, so a bare red is not accepted as evidence.
- [x] 2.12 Non-vacuity: assert the synthetic fixtures actually have the shape the cases claim (the
      self-consistency-assert idiom at `test_premerge_assert.sh:525-530`).

## 3. `premerge-assert.sh` integration — advisory only, NO verdict change

- [x] 3.1 Resolve `base-staleness.sh` from the script's OWN directory, no env override (D7/#3312).
      **Surface:** `scripts/flow/premerge-assert.sh` stdout.
- [x] 3.2 Print its finding on `PREMERGE: ADVISORY` lines. Never alter the exit code; an absent, failing,
      or `UNMEASURED` advisory is reported and non-fatal (D6).
- [x] 3.3 **Retain** the three `PREMERGE: SCOPE` lines and the literal `#3650`; extend by one line pointing
      at the advisory.
- [x] 3.4 Extend `scripts/tests/test_premerge_assert.sh`: advisory-printed case; broken-advisory-non-fatal
      case; extend Case 39 (`:842-867`) so the retained SCOPE wording stays pinned.

## 4. Gate wiring

- [x] 4.1 Register `scripts/tests/test_base_staleness.sh` in `run_tooling_tests`
      (`scripts/agent-gate.sh:10385`), including the echoed command list at `:11935`.
      **Surface:** the full gate's `tooling-tests` component.
- [x] 4.2 Confirm a failing assertion in the new suite makes `tooling-tests` — and the full gate — FAIL.
      Do NOT add it to `--lite` or `DELTA_COMPONENTS`.

## 5. Doctrine (same change, per CLAUDE.md)

- [x] 5.1 `CLAUDE.md:1052-1062` — describe the advisory, its non-blocking slice-1 status, the
      `UNMEASURED`-is-stale contract, and the declared non-exhaustiveness. Keep the merge-result gap open
      and name slice 2's issue.
- [x] 5.2 `scripts/flow/premerge-assert.sh:99-116` header residual 3 — same, and keep the residual.
- [x] 5.3 `.claude/agents/flow-closer.md:210-216` and `.claude/skills/flow-address/SKILL.md:76` — same.
- [x] 5.4 Do NOT edit `openspec/changes/archive/**` (historical).

## 6. Certification

- [ ] 6.1 `scripts/agent-gate.sh --lite` green each fix round, summary-file redirect (#2079).
- [ ] 6.2 `rust-reviewer` + sanctioned roborev (`scripts/flow/roborev-review.sh --agent codex --model
      gpt-5.6-sol`) on the lite-green diff, BEFORE any full gate (#2086). Push first.
- [ ] 6.3 Open PR with `Refs #3650` — **NOT `Closes`**: the issue stays open for slice 2.
- [ ] 6.4 `flow-closer`: ONE full gate of record → `spec-auditor` C → final roborev → `premerge-assert`
      → `gh pr merge --auto --squash --delete-branch`.
- [ ] 6.5 File slice 2 (merge-result gate mode + fail-closed enforcement + disclaimer update) as its own
      issue, and the dependency-closure blast radius as another. Reference both in the PR body.
- [ ] 6.6 Telemetry stamped with `--slice` (issue stays OPEN, `closed_at: null`) per #3550/#3559.
