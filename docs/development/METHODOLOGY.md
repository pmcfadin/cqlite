# CQLite Development Methodology

How work flows through CQLite: from an idea to merged, verified code. This is a
**spec-driven, agent-orchestrated, gate-enforced** workflow built on Claude Code. It is
designed so that most of the loop runs unattended, and a human is pulled in only for
product decisions.

> **Status legend:** **[live]** = in use today · **[rolling out]** = adopted, being
> piloted on the v0.13 cycle. The methodology is documented as the standard; the
> rolling-out pieces will be live as the pilot lands.

For the research and rationale behind these choices, see
[`docs/automation-overhaul-2026.md`](../automation-overhaul-2026.md). For the day-to-day
agent rules, see [`CLAUDE.md`](../../CLAUDE.md).

---

## Principles

1. **A spec is the source of truth, not the issue.** Requirements live in a durable,
   queryable spec that outlives any single task. Issues are the *execution ledger*, not
   the contract.
2. **The agent verifies its own work with a check it cannot trivially game.** A
   deterministic gate (build + lint + tests + smoke) must pass before any task is "done."
3. **The author is not the reviewer.** Implementation is reviewed in a *fresh context* by
   independent agents (and a second model family), plus audited against the spec.
4. **Isolate parallel work.** Each concurrent stream runs in its own git worktree so
   edits never collide.
5. **Humans decide product; agents decide implementation.** Ambiguous scope, conflicting
   requirements, and tradeoffs are escalated — never guessed.

---

## The layers

| Layer | Tool | Status | Role |
|-------|------|--------|------|
| **Spec / contract** | [OpenSpec](https://github.com/Fission-AI/OpenSpec) (`openspec/specs/`) | [rolling out] | Durable source of truth; what the product must do |
| **Execution ledger** | GitHub issues (epics + sub-issues) | [live] | Task tracking, assignment, status |
| **PM orchestrator** | `/prioritize`, `/pm-status`, `/start-epic` skills | [live] | Plan, prioritize, coordinate; conversational interface |
| **Orchestration engine** | Claude Code **Workflows** (`.claude/workflows/*.js`) | [planned] | Deterministic, code-driven fan-out/verify/synthesize for analysis, audit, and review phases |
| **Process discipline** | [superpowers](https://github.com/anthropics/superpowers) skills | [live] | Brainstorming, writing-plans, TDD, systematic-debugging |
| **Implementation** | Claude Code subagents in a worktree fleet | [live] | Write the code, one stream per issue/module |
| **Review** | roborev + `rust-reviewer`, `spec-auditor`, `coverage-reviewer` | [live] | Fresh-context adversarial review + spec/coverage audit |
| **Gate** | `scripts/agent-gate.sh` via the `TaskCompleted` hook | [live] | Deterministic pass/fail: fmt, clippy `-D warnings`, tests, smoke |
| **Notifications** | Slack (Salesforce workspace) via `Notification`/`Stop` hooks | [rolling out] | Reach a human when stuck or a decision is needed |

`openspec/` lives at the **repo root** (alongside `.github/`, `.claude/`), separate from
`docs/`. The split is deliberate: `openspec/specs/` answers *"what must the product
do?"*; `docs/` (the [definitive guide](../sstables-definitive-guide/), API references,
design history) answers *"how does it work / how was it built?"* Reference and
design-history docs stay in `docs/` and do **not** move into OpenSpec.

---

## Orchestration model

> **Status: [planned] — nothing in this section is built yet.** It specifies the target
> orchestration approach so the complete picture is agreed before any workflow is
> authored. The other layers above continue to operate as marked.

CQLite orchestration is built around **Claude Code Workflows** — deterministic JavaScript
scripts that coordinate subagents — used *in an orderly fashion*: as committed, named,
reviewable assets rather than ad-hoc dispatch. The guiding principle: **known-shape
orchestration belongs in code, not in a model's moment-to-moment judgment.**

### The four orchestration primitives, and the boundary between them

Claude Code offers four ways to run multiple agents. We assign each a specific job:

| Primitive | What it is | We use it for | Status |
|-----------|-----------|---------------|--------|
| **Workflow** | Deterministic JS script (`agent()`, `parallel()`, `pipeline()`, `phase()`) orchestrating subagents; runs in the background, returns structured results | **Analysis, audit, review, synthesis** — any known-shape fan-out + verify + synthesize | [planned] |
| **Worktree fleet / subagents** | Subagents dispatched into isolated git worktrees | **Implementation** — long, stateful, gate-looped coding that commits | [live] |
| **Agent Teams** | Peer Claude instances that message each other via a shared task list | **Attended interactive bursts** where agents must discuss each other's work | [rolling out] |
| **superpowers dispatch** | Skill-driven, model-coordinated subagent dispatch | Process discipline within a single stream (plan/TDD/debug) | [live] |

**The boundary that matters:** *Workflows orchestrate the analysis/review/audit/decision
layer; the worktree fleet does implementation.* Workflow agents are one-shot (no mid-run
back-and-forth, no human interaction during a run), so they are a poor fit for stateful
coding that loops on the gate. We do **not** turn implementation into a workflow.

### How Workflows fit the methodology

- **Skills are the thin interface; workflows are the engine underneath.** A skill such as
  `/audit-spec` or `/start-epic` *invokes* a saved workflow by name. A human talks to the
  PM; the PM runs deterministic workflows.
- **The PM chains workflows across turns** — one workflow per phase, reading each result
  before launching the next — staying in the loop without babysitting individual agents.
- **One workflow per phase, not one mega-workflow.** Understand, audit, review, and
  synthesize are separate scripts so each is independently reviewable and re-runnable.

### How a Workflow works (technical specification)

A workflow is a plain-JavaScript script (not TypeScript) that begins with a pure-literal
`export const meta = { name, description, phases }` block, followed by a body using these
hooks:

- **`agent(prompt, opts?)`** — spawn one subagent. Returns its final text, or — with
  `opts.schema` (a JSON Schema) — a validated structured object (validation happens at the
  tool layer, so the model retries on mismatch). Key `opts`: `label`, `phase`, `schema`,
  `model`, `effort`, `agentType` (reuse a `.claude/agents/*` definition, e.g.
  `spec-auditor`), and `isolation: 'worktree'` (only when agents mutate files in parallel).
- **`pipeline(items, ...stages)`** — run each item through all stages independently, **no
  barrier between stages**. This is the default for multi-stage work; wall-clock = slowest
  single chain, not sum-of-slowest-per-stage.
- **`parallel(thunks)`** — run concurrently with a **barrier** (awaits all). Use only when
  a stage genuinely needs *all* prior results at once (dedup/merge, early-exit-on-zero).
- **`phase(title)`**, **`log(msg)`** — progress grouping and narration shown in
  `/workflows`.
- **`workflow(name, args)`** — run a saved workflow as a sub-step (nesting one level only).
- **`args`** — the parameter value passed at invocation (epic number, file list,
  capability) so one script serves many runs.

Execution facts: workflows run in the **background** (a notification fires on completion);
concurrent `agent()` calls are capped at `min(16, cores − 2)` with a 1000-agent lifetime
backstop; a single `parallel`/`pipeline` call takes at most 4096 items; `Date.now()` /
`Math.random()` are unavailable (they would break resume — pass timestamps via `args`); and
a run can be resumed via its `runId` (unchanged `agent()` calls return cached results).

### Rules for orderly workflows

1. **Saved and committed** in `.claude/workflows/` — versioned, diffable, reviewed like
   skills and agents. *This is the single biggest shift from ad-hoc dispatch.*
2. **Declare `meta.phases`** so progress is legible in `/workflows`.
3. **Use `schema`** for phase hand-offs — structured data, no fragile text parsing. Keep
   early discovery stages text-returning if a schema would be brittle (the generic
   deep-research workflow once failed on an over-strict internal schema; owning our scripts
   avoids that).
4. **`pipeline` by default**, `parallel` only for a genuine cross-item barrier.
5. **Bake in adversarial verification** — a refute-stage so "found" ≠ "reported." Findings
   are confirmed in a fresh context before they reach a human.
6. **No silent caps** — if a workflow bounds coverage (top-N, sampling), it `log()`s what
   it dropped.
7. **Parameterize with `args`** — never hard-code an epic number or file list into a saved
   script.

### Planned workflows

These map directly onto the methodology phases. All **[planned]** — specified here, not
yet authored:

| Workflow | Shape | Feeds |
|----------|-------|-------|
| **`spec-audit`** | One reader per `openspec/specs/` capability → audit each against code → adversarially verify each gap → ranked drift report | The nightly drift sweep (files confirmed gaps as issues) and the `spec-auditor` |
| **`review-changes`** | Review the branch diff across dimensions (correctness / perf / safety / parity) → verify each finding in a fresh context → synthesize | Complements roborev with a deterministic multi-lens pass |
| **`epic-plan`** | Read all child issues of an epic in parallel → produce a dependency-ordered plan | `/start-epic`, to drive the worktree fleet |

The canonical shape for all three: `pipeline()` over the items, a `schema` on each
`agent()`, and an adversarial verify stage before synthesis.

---

## The lifecycle

```
idea ──► spec (OpenSpec change) ──► epic + child issues ──► implement (worktree)
                                                                   │
                                                                   ▼
                                                    gate (agent-gate.sh) ── fail ─┐
                                                                   │ pass         │ fix
                                                                   ▼              │
                                          review (roborev) + spec-auditor + ◄─────┘
                                                  coverage-reviewer
                                                                   │ sign-off
                                                                   ▼
                                              merge ──► sync spec (/opsx:sync) ──► archive
```

1. **Idea → spec.** A new capability or change starts as an OpenSpec *change*
   (`/opsx:explore` to think it through, `/opsx:propose` to generate `proposal.md`,
   `design.md`, `tasks.md`, and delta specs). The proposal is the durable design
   artifact. **[rolling out]**
2. **Spec → epic.** The PM turns the change into a GitHub **epic** with **child issues**,
   one per `tasks.md` item, each carrying its acceptance criteria. (OpenSpec has no native
   issues bridge, so this glue is owned by the PM.) **[live for issues; glue rolling out]**
3. **Implement.** `/start-epic <n>` spawns implementer subagents (≤4 concurrent, one per
   issue/module, each in its own worktree) plus a `spec-auditor` and `coverage-reviewer`.
   Implementers follow the superpowers process (plan → TDD → implement) and **commit
   often** so reviews land while context is fresh. **[live]**
4. **Gate.** A task cannot be marked done until `scripts/agent-gate.sh` passes — `cargo
   fmt`, `clippy -D warnings`, core/integration/write/CLI tests, minimal-features build,
   and smoke — emitting a machine-checkable summary block. Enforced by the `TaskCompleted`
   hook (exit 2 blocks completion). **Paste the summary block verbatim; ad-hoc `cargo`
   runs do not count as "the gate passed."** **[live]**
5. **Review.** roborev (a second model family) reviews each commit/branch; clear its
   findings (`/roborev-fix`) before handing an issue off. **[live]**
6. **Audit.** The `spec-auditor` confirms the implementation meets the spec; the
   `coverage-reviewer` confirms tests are *meaningful*, not merely present. **[live]**
7. **Merge → sync.** On merge, `/opsx:sync` folds the change's delta specs into
   `openspec/specs/` so the source of truth never lags the code, then the change is
   archived. **[rolling out]**

---

## Definition of done

An issue is done **only** when all hold (see [`CLAUDE.md`](../../CLAUDE.md)):

1. The deterministic gate passes (tests, coverage, no open roborev failures).
2. The `spec-auditor` confirms the acceptance criteria are met.
3. The `coverage-reviewer` confirms the tests are meaningful.
4. roborev is clean.

The PM may close an issue when its acceptance criteria are met and the work is clearly
complete (e.g. a merged linked PR), with a traceable closing comment. The PM **never**
closes an epic, changes scope/title, or makes a product decision alone — those go on a
**NEEDS YOU** list for a human.

---

## Guardrails

- **No reward-hacking the gate.** Tests are committed *before* implementation; agents may
  not weaken, skip, or delete a test to make the gate pass. Known-flaky tests
  (`flush-throughput`) and pre-existing gate gaps (python-bindings) are protected — fix
  the implementation, not the check. **[guard rolling out]**
- **Blast-radius limits.** Destructive commands and force-pushes to `main` are blocked at
  the tool layer. **[rolling out]**
- **Cost discipline.** Model-tier the fleet (Opus lead / Sonnet implementers / Haiku
  reviewers); multi-agent work runs 3–15× the tokens of a single session.
- **No heuristics.** Decode from authoritative metadata only — see the
  [no-heuristics mandate](https://pmcfadin.github.io/cqlite/agents-developing/no-heuristics/).

---

## Using this workflow yourself

To run CQLite's loop on your own machine:

1. **Claude Code** — the orchestration host.
2. **superpowers** plugin — process skills (brainstorming, plans, TDD, debugging).
3. **roborev** — the review daemon (`.roborev.toml` is committed; it reviews per branch).
4. **OpenSpec** — `npm install -g @fission-ai/openspec`, then `openspec init` at the repo
   root. **[rolling out]**
5. The committed **skills** (`.claude/skills/`), **subagents** (`.claude/agents/`), and
   **hooks** (`.claude/hooks/`) come with the repo. The gate is `scripts/agent-gate.sh`.
6. **Notifications (optional)** — wire a `Notification`/`Stop` hook to your own Slack
   webhook so you're pinged when a run needs you. **[rolling out]**

Then: talk to the PM (`/prioritize`, `/pm-status`), kick off work with `/start-epic <n>`,
and let the gate + review + audit loop run. You step in only when the **NEEDS YOU** list
has something for you.
