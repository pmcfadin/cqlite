# Design: harden-parity-report-staleness

## Context

`docs/reports/cassandra-test-parity.md` is a **committed derived artifact**: it is rendered from
`test-data/cassandra-parity-manifest.yml` (status fields, evidence types, scenario→test mapping) by
`cassandra-parity report`. `.github/workflows/cassandra-parity.yml` guards it with `report --check`,
which fails when the committed file differs from a fresh render. The guard is correct; the failure mode is
*where* the drift is introduced.

### The hazard, precisely

A derived artifact committed next to its source regresses on a **semantic merge** that git cannot detect:

```
main@t0  ── manifest M0, report R(M0)            (consistent)
 ├─ PR A: manifest M0→Ma, report R(M0)→R(Ma)     green vs base t0
 ├─ PR C: manifest M0→Mc, report R(M0)→R(Mc)     green vs base t0
 C merges → main@t1: manifest Mc, report R(Mc)
 A merges (no textual conflict) → main@t2: manifest = Ma∪Mc (lines union),
                                            report  = R(Ma)        ← stale: ≠ R(Ma∪Mc)
```

Both PRs pass their own `--check` (each green against `t0`). The staleness exists **only at `t2`**, on
`main`. A per-PR check is therefore structurally incapable of catching it. Empirically confirmed for the
2026-06-30 incident: `#1308`'s post-merge push run was the first red, at the exact instant it landed,
after `#1302`/`#1306` (both manifest-count-changing) had merged ahead of it.

## Goals / Non-Goals

- **Goal:** No stale committed report persists on `main` undetected; correcting it does not require a human
  to manually regenerate and does not hold the whole PR queue red.
- **Goal:** Also catch the simpler single-PR "forgot to regenerate" case (the #1330 class) earlier — at
  the local gate, before push.
- **Non-goal:** Re-architect the report away from being committed; change report content; widen path
  filters.

## Decisions

### D1 (OWNER DECISION — the mechanism). How do we defeat the post-merge race?

| Option | Mechanism | Pros | Cons |
|---|---|---|---|
| **A. Self-healing PR (recommended)** | Push-to-`main` job: on `--check` stale, auto-open a regeneration PR (`cassandra-parity report` + commit) via the GitHub API; never pushes directly to protected `main`. | Zero added per-PR CI cost; no queue serialization; `main` self-corrects in one cycle; honors "main requires PR review". | A brief red window on `main` until the auto-PR merges; needs a token with PR-create scope; the auto-PR itself must pass CI. |
| **B. Require up-to-date before merge** | Branch protection / merge queue forces each parity-touching PR to rebase on tip and re-render before merge. | Bulletproof — the race cannot occur. | Serializes merges; heavy on a busy queue during the "CI overloaded" sprint; affects all PRs, not just parity ones. |
| **C. Stop committing the report** | Generate on-demand (docs build / artifact); drop it from the tree. | Eliminates the drift surface entirely. | Larger refactor; unknown downstream link/tooling deps; out of scope here (Non-goal). |

**Recommendation: A (self-healing PR), plus the D2 local check.** A fits the current constraints (busy
queue, CI-overloaded sprint) with no merge serialization, and matches the repo rule that `main` requires a
reviewed PR. B is recorded as the fallback to escalate to if self-healing proves flaky. C is a future
option, not this change. **This is the decision the owner makes at approval.**

### D2 (in scope regardless of D1). SKIP-aware `parity-report` agent-gate component.

Add a `parity-report` component to `scripts/agent-gate.sh` (and its `COMPONENTS` list) that runs
`cargo run -p cassandra-parity -- report --manifest test-data/cassandra-parity-manifest.yml
--output docs/reports/cassandra-test-parity.md --check`. SKIP-aware like the existing `delivery-telemetry`
/ python components: if the `cassandra-parity` crate or the manifest is absent (e.g. a minimal checkout),
the component SKIPs rather than FAILs. This catches the single-PR forgotten-regen case at the local gate —
the layer the post-merge race fix does not cover — and is cheap (no Docker/dataset; reads manifest + tree).

### D3. Keep the existing push + PR `parity-manifest --check` as the detector.

The current `--check` step is the **trigger** for self-healing (D1-A) and remains the authoritative
staleness oracle. We do not remove or weaken it; we change only what happens **after** it reports stale on
`main` (open a fix PR instead of leaving `main` red for a human).

### D4. The self-healing PR must be idempotent and non-recursive.

The auto-opened regen PR touches only `docs/reports/cassandra-test-parity.md`. The push-to-`main` healing
job must not loop: it opens at most one open regen PR at a time (detect an existing open
`auto/parity-report-regen` PR/branch and update it rather than stacking duplicates), and the regen PR's own
merge produces a `main` where `--check` is green — terminating the cycle.

## Risks / Trade-offs

- **Auto-PR token scope (RESOLVED — roborev #1338).** A PR opened with the default `GITHUB_TOKEN` does
  NOT trigger `pull_request` CI (GitHub's recursion guard), so the regen PR would land with no checks and
  be unmergeable via the green-check flow. Resolution (owner-approved): authenticate the heal job's
  checkout/push/`gh pr create` with a dedicated PAT/GitHub-App token, repo secret `PARITY_HEAL_TOKEN`
  (`contents` + `pull-requests` write), mirroring `PROJECTS_TOKEN` in `project-board-sync.yml`. When the
  secret is absent the job SKIPs with a `::notice::` (report regenerated manually) rather than opening a
  check-less PR — so the workflow is safe to merge before the secret is provisioned and activates fully
  once it is.
- **Brief red window.** Between a stale merge and the auto-PR landing, `main`'s `parity-manifest` is red,
  so the queue is briefly blocked — but recovery is automatic (minutes), not a manual fire drill. The D2
  local check shrinks how often this triggers at all.
- **Escalation path.** If self-healing proves noisy/flaky, D1-B (up-to-date enforcement) is the documented
  bulletproof fallback — no redesign needed, just a branch-protection toggle.

## Migration / Rollout

Additive: a new agent-gate component (SKIP-aware, so existing flows are unaffected when the tool is
absent) and a new/extended workflow job. No changes to report content, the manifest schema, or any
parser/writer path. Doctrine note lands in the same change.
