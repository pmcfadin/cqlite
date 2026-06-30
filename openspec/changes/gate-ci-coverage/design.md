## Context

The authoritative gate has zero CI coverage (proposal.md, with file:line). The design question the issue
defers is **which lane runs the gate, and how strict**. Three options, weighed against the audit facts
(full gate ~25 min; `--only` is `PARTIAL`/doesn't count; `cqlite-core/**` is touched constantly; 8 cron
lanes already exist; dataset preflight required).

## Goals / Non-Goals

- **Goal:** a CI lane reds when a gate component breaks (acceptance: break `node-bindings` in
  `agent-gate.sh` → a lane reds), and a path-independent full-gate run exists.
- **Non-goal:** redefining the gate, or adding the full gate as a required check to every core PR.

## Decision — RECOMMENDED (owner to approve / redirect at Seam 1)

**Option C: scoped PR-triggered gate lane + nightly full-gate cron backstop.**

1. **PR lane `gate.yml`**, triggered ONLY on the gate-defining inputs:
   `scripts/agent-gate.sh`, `scripts/tests/**` (its self-tests), `bindings/**`, and its own workflow file.
   It runs the **full** `scripts/agent-gate.sh` (so it counts), with the dataset preflight
   (`fetch-datasets.sh` + `CQLITE_DATASETS_ROOT`) and the inline `Swatinem/rust-cache@v2` pattern. A
   failing component reds the PR check.
2. **Nightly cron** (a `schedule:` in the same workflow, slotted alongside the existing nightly lanes)
   runs the **full** gate path-independently as a backstop, plus `workflow_dispatch` for on-demand runs.

**Why C:**
- Directly satisfies acceptance: a PR editing `scripts/agent-gate.sh` (or `bindings/**`) that breaks a
  component reds the lane — the exact gap today.
- Avoids piling a ~25-min full gate onto **every** `cqlite-core/**` PR (Option A): core regressions are
  already caught by `ci.yml` + `node-ci`/`python-ci`, and `cqlite-core/**` changes on nearly every PR
  would make A's cost enormous and redundant.
- The nightly backstop gives a path-independent "counts as the gate" run within 24h, catching anything
  the narrow path filter misses, and fits the 8 existing cron lanes.

### Alternatives considered (the owner may prefer one)

- **Option A — run the full gate on every `scripts/** + bindings/** + cqlite-core/**` PR.** Strongest
  immediacy (every component change verified pre-merge) but **~25 min added to almost every PR** (core is
  touched constantly) and largely redundant with existing lanes. Rejected for cost; recommend only if the
  owner wants the gate as a hard pre-merge contract everywhere.
- **Option B — nightly cron only (no PR trigger).** Cheapest (one run/day) and path-independent, but a
  gate-only PR merges **unverified** for up to 24h. Weaker on the literal acceptance ("a PR … reds a
  lane"). Recommend only if PR-time CI budget is the dominant concern.

### Strictness (owner decision)

- **Recommended:** the PR `gate.yml` check is **required** for PRs that touch its narrow path set (it only
  fires on gate/binding inputs, so it is not a tax on unrelated PRs); the nightly is a **backstop that
  fails loudly** (surfaces on the Actions dashboard; optional follow-up to auto-file an issue, out of
  scope here). The owner may instead make the PR lane advisory (non-blocking) if preferred.

## Risks / Trade-offs

- **Cost:** even scoped, a full-gate PR run is ~25 min; mitigated by the narrow path filter (gate/binding
  inputs only) + Rust cache. The nightly adds one scheduled run/day.
- **Flaky lanes inside the gate** (e.g. `test_flush_throughput`, py3.9) could red the lane; mitigated by
  the gate's own known-flaky handling and `workflow_dispatch` re-run. Not introducing new flakiness.
- **Dataset availability:** the lane must fetch the pinned datasets; a 0-rows-when-present condition must
  remain a failure (existing doctrine), not a skip.

## Migration

None. Additive workflow only; no change to the gate script or existing lanes.
