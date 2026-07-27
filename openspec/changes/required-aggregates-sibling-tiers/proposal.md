# `required` aggregates sibling tiers — CI green must mean the relevant CI ran

## Why

Issue #2910. Branch protection requires exactly one status context, `required`, and `required` is a
**self-contained job** (`jobs.required` / `name: required` in `.github/workflows/pr-gate.yml`). It runs
its own steps and nothing else. A GitHub Actions job cannot `needs:` a job in another workflow, so every
tier that lives in a separate workflow — `flight-ci.yml`, the parity lanes, coverage, every label-gated
suite — is **invisible to `gh pr merge --auto`**.

Verified on this branch:

- `.github/branch-protection.json` → `required_status_checks.contexts: ["required"]`, `enforce_admins: true`.
- `pr-gate.yml` has `permissions: contents: read` and **no** check/status API call anywhere in the repo.
- 25 workflows carry a `pull_request` trigger; most are `paths:`-filtered, several are label-gated
  (`ci:flight-full`, `ci:bindings-full`, `ci:ingest-full`, `ci:broad`, `ci:perf`, `ci:docs-full`).

So a PR can land fully green with its most important integration test still pending, failed, or never
triggered. On PR #2906 the end-to-end test constituting the change's **wiring evidence**
(`cqlite-flight/tests/issue_2825_max_batch_bytes_e2e.rs`) ran in neither the local gate's standing
components nor `flight-ci.yml`'s PR tier — it needed a `ci:flight-full` label the PR did not carry. A
`spec-auditor` caught it by hand. Absent that, `--auto` would have landed a headline feature with no
executed end-to-end proof while every local signal was green.

This is flow-meta #1310 (`agent-gate.sh` PASS != CI green) one level worse: **CI green does not mean the
relevant CI ran.**

## What changes

The owner's decision (binding, #2910): **`required` polls the PR's sibling check runs and fails closed**
if any tier expected for this diff is failed, pending, or absent. One required context is preserved — no
branch-protection change, and no exposure to the `pr-gate.yml:27-34` deadlock trap where a path-filtered
required check that never fires blocks affected PRs forever.

- **A declared registry** — `.github/ci-gating-tiers.yml` — names every gating tier by workflow file and
  check-run context. It is the single source of truth for what `required` waits on.
- **Registered tiers always emit.** Each registered workflow drops its trigger-level `paths:` filter (the
  `ci.yml` `__required_ci_context_never_matches__` sentinel pattern) and moves applicability into a cheap
  always-run classifier job (the `observability-gate.yml` `classify` pattern) that emits its declared
  context in **every** case — including "not applicable to this diff", reported as an explicit success.
  Absence therefore stops being ambiguous: an absent registered context is always an error.
- **A diff-based mandate overrides the label gate.** For a registered tier, the classifier decides from
  the diff whether the tier is *mandatory*; the `ci:*` label stays an opt-in accelerator for diffs that do
  not mandate it. The #2906 case (`cqlite-flight/**` touched) runs the e2e tier with or without the label.
- **Enrolment is forced.** `scripts/ci/validate-workflows.rb` — already executed *inside* the `required`
  job — gains a rule: every `pull_request`-triggered workflow must be either registered as a gating tier
  or listed in the registry's `exempt:` block with a reason and an issue link. A new tier that forgets to
  enrol reds `required`.
- **A new aggregation step/job** in `pr-gate.yml` (`scripts/ci/aggregate-required-tiers.sh`) polls
  `GET /repos/{owner}/{repo}/commits/{pr_head_sha}/check-runs?filter=latest`, excludes its own run's job
  ids, and fails closed on any registered context that is failed, non-terminal at the deadline, or absent.
  `pr-gate.yml` gains `checks: read` + `actions: read` (it has neither today).
- **Doctrine** — `CLAUDE.md`'s autonomy section and `website/src/content/docs/agents-developing/gate-contract.md`
  state accurately what `required` now covers. Today both imply a coverage that does not exist.
- **Offline testability** — the aggregator reads check-run JSON from an injectable source so its polling
  logic is exercised against synthetic states (all-pass, one-pending, one-failed, one-absent-and-registered,
  one-absent-and-not-registered) in `scripts/tests/`, wired into the gate's `tooling-tests` component,
  with a proven-failing case per state.

Milestone: maintenance / delivery infrastructure. Routing: **design-driven** (CI topology + doctrine);
there is no external oracle for "which tiers gate a merge".

## Non-goals

- **No branch-protection change.** `contexts` stays `["required"]`; `.github/branch-protection.json` is
  untouched. Adding tier contexts directly is the rejected option 2 from #2910 (deadlock trap).
- **Not moving tiers into `agent-gate.sh`** (#2910 option 3). Whether a given wiring-evidence test also
  belongs in the standing local gate is a per-tier call tracked separately (#1269).
- **Not making advisory lanes blocking.** An unregistered red check run (perf advisory, docs recipe smoke)
  deliberately does NOT block; only registered contexts gate.
- **Not re-tiering the parity lanes.** `docs/development/parity-ci-tiers.md`'s 5-tier parity taxonomy is a
  different axis and is not merged into this registry.
- **No change to the local gate's component set**, the no-heuristics mandate, binding surfaces, or the
  <128MB memory budget — this change touches CI topology, one shell script, and doctrine only.

## MIGRATION NOTE — read before merging (#2910 round 3, R1)

This change makes `main` register the `flight` gating tier, and `required` reads the registry from a pull
request's **base ref** while the tier's emitting job comes from the tree the event ran. Those are two
different trees, so at the instant this merges they can disagree.

**What actually happens on merge.** For a `pull_request` event GitHub takes workflow definitions from the
**merge commit**, so an open PR that does not touch `flight-ci.yml` picks up the new trigger, classifier
and `Flight tier gate` job on its next event with no rebase. An unrebased head that somehow does not
reds `pr-gate-core` first (`validate-workflows.rb` cannot find `.github/ci-gating-tiers.yml`), which reds
`required` on the core result — wrong-but-fast, never an hour of silence.

**What is nonetheless handled by construction.** The aggregating job now checks out `github.sha` (the merge
commit) sparse and read-only, and if the base registers a tier that tree provably cannot emit — workflow
absent, no PR trigger, `types:`/`branches:` excluding this event, or no job with that name — `required`
fails on the **first poll** with a diagnostic naming both remedies:

- **rebase** the pull request onto the base branch; or
- apply **`ci:waive:flight`** if the tier is deliberately being renamed/retired (a registry change only
  takes effect once merged).

Inconclusive evidence (unparseable workflow, computed job name, unavailable checkout) never produces that
verdict, and the verdict is never a pass — a PR controls that tree, so "cannot emit, therefore green" would
be a bypass.

**No short absent-deadline** was added, deliberately: a tier's gate job `needs:` every other job in its
workflow, so its check run legitimately does not exist for as long as the tier takes to run. A timer on
"absent" would red exactly the pull requests that genuinely mandate the tier. Speed comes from the provable
property, evaluated immediately.

The most likely real migration state is a **follow-up PR that renames this tier's context** — that shape is
covered by a test and by the waiver.

Full reasoning: `openspec/changes/required-aggregates-sibling-tiers/design.md`, section
"What behaves differently at the MOMENT OF MERGE".
