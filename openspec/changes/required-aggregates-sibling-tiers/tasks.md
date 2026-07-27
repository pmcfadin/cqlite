# Tasks — `required` aggregates sibling tiers

## 1. Gating-tier registry (surface: `.github/ci-gating-tiers.yml`)
- [x] 1.1 Define the schema: `tiers[]` with `id`, `workflow`, `context`, optional `wait_minutes`,
      optional `mandate_paths` (documentation of the tier's own predicate); `exempt[]` with `workflow`,
      `reason`, `issue`.
- [x] 1.2 Populate `exempt[]` for the 23 current `pull_request` workflows that are neither the
      aggregator nor a gating tier (25 carry the trigger), with a one-line reason each, so
      the enrolment rule lands green; promote to `tiers[]` only where the tier is correctness evidence.
- [x] 1.3 Register the motivating tier first — `flight-ci.yml`'s full test tier (#2910 / PR #2906) — and
      name the exact context string it emits.

## 2. Enrolment enforcement (surface: `scripts/ci/validate-workflows.rb`, run in `pr-gate-core`,
##    the job `required` needs and treats as an unconditional failure unless it succeeded)
- [x] 2.1 New rule: every `pull_request`/`pull_request_target` workflow is in `tiers[]` or `exempt[]`;
      an exemption without `reason` + `issue` fails.
- [x] 2.2 Structural rules for a registered tier: the workflow has a `pull_request` trigger at all; no
      blocking `paths:`/`paths-ignore:`/`branches:` (the `__required_ci_context_never_matches__` sentinel
      is allowed); `types:` covers `opened`+`synchronize` and stays within the aggregator's observed set;
      exactly one job emits the declared context and its condition is EXACTLY `always()`; for every
      dependency some step both reads `needs.<job>.result` and can exit non-zero; dangling entries fail;
      `pr-gate.yml` may not be registered.
- [x] 2.4 The aggregator must fire on `labeled`/`unlabeled`, or the `ci:waive:<tier-id>` break-glass is
      unreachable on a wedged PR.
- [x] 2.3 Assert the aggregation deadline is strictly less than the aggregating job's `timeout-minutes`.

## 3. The aggregator (surface: `scripts/ci/aggregate-required-tiers.sh`)
- [x] 3.1 Read the registry; poll `GET /repos/{o}/{r}/commits/{head_sha}/check-runs?filter=latest`
      (paginated) for `github.event.pull_request.head.sha`; keep the highest check-run id per context.
- [x] 3.2 Self-exclude by run identity: job ids from `GET /actions/runs/${GITHUB_RUN_ID}/jobs`, plus a
      details-URL fallback on the same run id. No name matching.
- [x] 3.3 Fail closed on failed / non-terminal-at-deadline / absent; ignore unregistered contexts;
      back off 15s → 60s; expire into a FAILURE naming every non-terminal tier.
- [x] 3.4 Waivers: honour `ci:waive:<tier-id>` for absent/pending only, never for a terminal failure;
      no blanket waiver; emit a `::warning::` + summary line per waiver.
- [x] 3.5 Emit a job summary listing `(context, check-run id, status, conclusion, run URL)` per tier.
- [x] 3.6 Injectable inputs for offline runs: check-run JSON source, registry path, deadline, poll budget.

## 4. `pr-gate.yml` wiring (surface: `.github/workflows/pr-gate.yml`)
- [x] 4.1 Split into `pr-gate-core` (today's steps verbatim) and `required` (`name: required` UNCHANGED,
      `needs: [pr-gate-core]`, `if: always()`), so the branch-protection context name is untouched.
- [x] 4.2 `required` fails whenever `needs.pr-gate-core.result != 'success'`; never skipped; keep the trigger
      unfiltered.
- [x] 4.3 Add `checks: read` + `actions: read` to `permissions` (today: `contents: read` only) and set the
      aggregating job's `timeout-minutes` above the aggregation deadline.
- [x] 4.4 Confirm `.github/branch-protection.json` is unchanged (`contexts: ["required"]`).

## 5. Tier conversion (surface: the registered tier workflows, starting with `flight-ci.yml`)
- [x] 5.1 Replace the trigger `paths:` filter with the always-fire sentinel; add an unconditional classifier
      job emitting the declared context (pattern: `observability-gate.yml` `classify`).
- [x] 5.2 Classifier computes the mandate from `git diff --name-only base...head`; a mandating diff runs the
      full tier regardless of the `ci:*` label; a non-mandating diff leaves the label as opt-in.
- [x] 5.3 Inapplicable → emit the context as an explicit success naming the reason; expensive jobs stay
      gated on the classifier output.

## 6. Empirical verification before the registry is populated
- [x] 6.1 VERIFIED against live PR #2976 before the registry was populated:
      `GET /repos/pmcfadin/cqlite/commits/<pull_request.head.sha>/check-runs?filter=latest` returned 23
      sibling check runs (18 `skipped`, 5 `success`), and each `check_run.id` equals the Actions job id
      while `details_url` carries `/actions/runs/<run_id>/job/<id>` — which is exactly what the
      run-identity self-exclusion relies on. Also confirmed there that a `paths:`-filtered workflow
      (`flight-ci`) produced ZERO check runs, i.e. only a filtered TRIGGER goes dark; a job skipped by an
      `if:` inside a workflow that did fire still emits a `skipped` check run.
- [x] 6.2 `pr-gate.yml` declares `permissions: contents: read` (narrowing below the repo default), so it
      gains `checks: read` (check-runs endpoint) + `actions: read` (run-jobs endpoint). Both are
      read-only scopes the default `GITHUB_TOKEN` receives on fork PRs, so no PAT is involved and this
      does not join the fail-loud-if-absent secret class. RESIDUAL: the fork-PR path itself cannot be
      exercised from an in-repo branch; the aggregator fails CLOSED (exit 2, `::error::`) if either
      endpoint is unreadable, so a permission gap would red the gate rather than open it.

## 7. Tests (surface: `scripts/tests/test_aggregate_required_tiers.sh`, `scripts/ci/tests/`)
- [x] 7.1 Synthetic check-run fixtures: all-pass, one-pending, one-failed, one-absent-and-registered,
      one-absent-and-not-registered, duplicate-context re-run (both directions), self-exclusion.
- [x] 7.2 A discriminating case per state: assert non-zero exit AND that the offending tier is named.
- [x] 7.3 Waiver cases: absent+waived passes; failed+waived FAILS; waiver scoped to one tier.
- [x] 7.4 Non-vacuity: an always-exit-0 stub aggregator turns the suite RED; an always-pass stub enrolment
      rule turns the `validate-workflows.rb` tests RED.
- [x] 7.5 No wall-clock assertions (#2642) — expiry via an injected already-expired deadline / zero poll
      budget; verify `scripts/tests/check-no-wallclock-asserts.sh` stays clean.
- [x] 7.6 Wire the suite into `scripts/agent-gate.sh`'s `tooling-tests` component.

## 7b. False-RED hardening (both failure directions are outages)
- [x] 7b.1 P1 waiver reachability: aggregating workflow subscribes to `labeled`/`unlabeled`; the
      aggregation re-reads the PR's current labels each poll (payload as fallback); a policy rule rejects
      an aggregator that does not observe label events.
- [x] 7b.2 P2 supersession: `cancelled`/`stale` are non-terminal while a replacement is plausible
      (positively detected via a higher check-run id) and fail at the grace lapse or the deadline;
      never waivable. Tier `types:` must stay within the aggregator's observed set.
- [x] 7b.3 P3 transient fetch failures retried under backoff; fail closed only on persistence.
- [x] 7b.4 P4 `types:` validated in both directions, plus the degenerate no-PR-trigger case.
- [x] 7b.5 P5 the emitting job's condition must be EXACTLY `always()`.
- [x] 7b.6 P6 structural failing-path check (reads the result AND can exit non-zero, comments and quoted
      strings stripped) replacing the `/exit 1/` substring match.
- [x] 7b.7 P7 the aggregator refuses an empty/unparseable `tiers:` itself.
- [x] 7b.8 P8 a waived ABSENT tier resolves immediately; a waived PENDING one still waits.
- [x] 7b.9 P10 lone check-run object parses; eval'd command inputs shape-validated.
- [x] 7b.10 Mutants for each, including the near-miss inverses that must NOT red.

## 8. Doctrine
- [x] 8.1 `CLAUDE.md` autonomy section: `required` aggregates the registered sibling tiers and fails closed
      on failed/pending/absent; arming `--auto` stays correct; tier-then-`required` re-run order.
- [x] 8.2 `website/src/content/docs/agents-developing/gate-contract.md`: same, plus the registry's location
      and the enrolment rule.
- [x] 8.3 `docs/ci/ci-tier-policy.md`: record that the sole required context now aggregates, and that new
      `pull_request` workflows must enrol or exempt.
- [x] 8.4 State explicitly that no step of the worker flow requires applying a tier label.

## 9. Gate / review / audit
- [x] 9.1 `--lite` green each fix round (summary-file redirect); review-first (rust-reviewer + roborev on the
      lite-green diff) before the one full gate.
- [ ] 9.2 Full `scripts/agent-gate.sh` once pre-merge inside `flow-closer`; C (`spec-auditor`) anchored to
      `openspec/changes/required-aggregates-sibling-tiers/specs/**`; final roborev clean.
