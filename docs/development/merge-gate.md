# The merge gate — `--auto`, the `required` check, and the tier registry

How a green PR actually lands. `CLAUDE.md` holds the rule (`gh pr merge --auto` is the only
sanctioned merge); this file holds the mechanism. Moved verbatim out of `CLAUDE.md` (#4092).

---

- **Autonomy — arm `--auto`, GitHub merges on green (default, #2667)**: the moment **local
  certification** is met — local gate PASS + **C** PASS (design-driven) + roborev clean — workers (and
  the lead) **arm auto-merge on their own PR** via `gh pr merge --auto --squash --delete-branch`
  (after the pre-merge SHA assert + `HOLD` re-read), then finalize. GitHub owns the CI-green wait and
  lands the PR the instant the `required` check passes — **never `ScheduleWakeup`-poll a PR's own CI**.
  Branch protection enforces the `required` check for admins too (`enforce_admins`), so `--auto` can
  never land against an unchecked head and bypass is impossible; a known-flake red gets
  `gh run rerun --failed`, never a bypass. This enforcement is load-bearing: if branch-protection
  settings change, this doc governs catching it (#2433). **`gh pr merge --auto` is the ONLY sanctioned
  merge — REST `PUT repos/OWNER/REPO/pulls/N/merge` is ABSOLUTELY FORBIDDEN (#3055)**: it merges
  *immediately*, bypassing the required-check wait branch protection exists to enforce, so it is never a
  GraphQL-throttle fallback. `--auto` is set-once/idempotent — on a throttle, **sleep and retry the same
  arm**. (The comment-post and PR-create REST fallbacks remain fine; only merge is forbidden.) **What a green `required` now covers
  (#2910)**: `required` is no longer only its own steps — it also polls the PR head's sibling check
  runs and **fails closed** on any tier declared in `.github/ci-gating-tiers.yml` that is failed,
  still pending at the aggregation deadline (60 min default), or **absent** (absence is an error,
  never "not applicable" — a registered tier always emits its context, reporting inapplicability as
  an explicit success). So arming `--auto` before the tiers finish stays correct: GitHub releases the
  merge on `required` going green, and `required` cannot go green until every registered tier has
  reported success. A **diff that mandates a tier runs it with or without the tier's `ci:*` label**,
  so **no step of the flow asks a worker to decide which tiers are out of band or to apply a label**.
  Adding a `pull_request` workflow without enrolling it in the registry (as a tier or an
  annotated exemption) reds `required`. Residual: a tier re-run **after** `required` is already green
  cannot be retracted by a finished job — **re-run the tier, then re-run `required`**, in that order.
  Break-glass is per-tier only (`ci:waive:<tier-id>`, owner action) and can excuse an absent or
  pending tier, **never** a failed one — applying it takes effect **without a re-run** (the
  aggregator re-reads live labels each poll) and **without restarting `pr-gate-core`** (label events
  queue rather than cancel, and skip the core, reusing the result already recorded for that head
  sha). A waiver is **bound to the head sha it was applied for**: a label survives a push, so after
  you push again it no longer short-circuits — the tier is polled and a failure it reports still reds
  the gate; **remove and re-apply the label** to waive the new head. Two further properties worth knowing: `required` evaluates the aggregator **and the registry
  from the PR's BASE ref**, so a registry/aggregator change lands only after it merges (rename a
  tier's context in a separate PR, or waive it) — the **same shape** as roborev reading
  `exclude_patterns` from the repo **root path** and snapshotting it at daemon start (#3229, above);
  generalized, **any PR whose subject is a config a daemon or gate reads from root cannot certify
  itself**, so plan its demonstration for after the merge; and a tier's mandate covers everything that reaches
  it at runtime — for Flight that includes `cqlite-core/**`, `test-data/**` and the Cargo manifests,
  so core-touching PRs run the Flight e2e tier. Finalize runs in-session when the required
  check is already green at arm time, else on a later wake confirming `state=MERGED`. Do NOT
  wait for the owner. Seam 1
  (spec approval) is the only standing human gate. Escalate and **hold the merge** ONLY for: a
  genuine design-call roborev finding, a scope/product question, an unmet/uncovered requirement, or
  work outside the issue — and obey any `HOLD: merge after #N` order.
