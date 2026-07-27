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
