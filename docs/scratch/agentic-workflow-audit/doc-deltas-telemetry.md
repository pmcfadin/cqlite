# Doc deltas — roborev severity (issue #2088), for phase-2 doctrine wiring

Tooling half of #2088 is done (schema + `delivery-telemetry.py` + the rubric page at
`docs/development/roborev-severity.md`). This note records the doctrine wording a
later phase-2 pass should thread into `CLAUDE.md` / `flow-implement` / the published
`agents-developing/roborev-findings` page, so the blocker/nit loop rule is load-bearing
in the pipeline, not just documented in isolation.

## Where it belongs

1. **`CLAUDE.md` → "Pre-roborev self-check" section** (the bullet list of recurring
   finding classes to pre-empt): add a lead-in sentence pointing at
   `docs/development/roborev-severity.md` for how findings are classified, and note
   that the listed classes below are all BLOCKER-severity by definition.

2. **`CLAUDE.md` → "Agent-team conventions"** (the "Clear roborev findings (run
   /roborev-fix) before handing an issue off" bullet): replace with the loop rule —
   *blockers fixed pre-merge (each re-triggers fix → `--lite` gate → re-review);
   nits batched into ONE linked follow-up issue, opened at merge time, and NEVER
   trigger a re-verify round.*

3. **`flow-implement` skill** (the roborev step of the implement loop): the step that
   currently says "clear roborev findings" should say "triage roborev findings per
   `docs/development/roborev-severity.md`: fix blockers, batch nits into one follow-up
   issue" — this is the actual mechanical change that saves the re-verify rounds the
   telemetry retro flagged.

4. **Published site page** `agents-developing/roborev-findings` — add the severity
   rubric (or a link to `docs/development/roborev-severity.md`) alongside the existing
   "recurring finding classes to pre-empt" list, since the site is the canonical
   source per CLAUDE.md's doctrine-mirroring convention.

## Loop rule (verbatim, for reuse)

> Blockers are fixed pre-merge, in the same round as everything else roborev found —
> each one re-triggers the normal fix → `--lite` gate → re-review loop. Nits never
> trigger a re-verify round: all nits from one PR's roborev pass are batched into ONE
> linked follow-up issue (labeled, referencing the PR) opened at merge time.

## Not done here (out of scope for the tooling PR)

- No `CLAUDE.md` edit — out of this PR's allowed file list.
- No `flow-implement` skill edit.
- No edit to the published `agents-developing/` site (external repo/build).
- Acceptance-criteria items "Rubric documented" and "Nit-batching rule documented" are
  satisfied by `docs/development/roborev-severity.md` itself; this note is the handoff
  for wiring that rubric into the *enforcing* doctrine surfaces above.
