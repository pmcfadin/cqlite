# Tasks — `required` aggregates sibling tiers

## 1. Gating-tier registry (surface: `.github/ci-gating-tiers.yml`)
- [ ] 1.1 Define the schema: `tiers[]` with `id`, `workflow`, `context`, optional `wait_minutes`,
      optional `mandate_paths` (documentation of the tier's own predicate); `exempt[]` with `workflow`,
      `reason`, `issue`.
- [ ] 1.2 Populate `exempt[]` for all 25 current `pull_request` workflows with a one-line reason each, so
      the enrolment rule lands green; promote to `tiers[]` only where the tier is correctness evidence.
- [ ] 1.3 Register the motivating tier first — `flight-ci.yml`'s full test tier (#2910 / PR #2906) — and
      name the exact context string it emits.

## 2. Enrolment enforcement (surface: `scripts/ci/validate-workflows.rb`, run inside the `required` job)
- [ ] 2.1 New rule: every `pull_request`/`pull_request_target` workflow is in `tiers[]` or `exempt[]`;
      an exemption without `reason` + `issue` fails.
- [ ] 2.2 Structural rules for a registered tier: no blocking trigger `paths:`/`paths-ignore:` (the
      `__required_ci_context_never_matches__` sentinel is allowed); an unconditional job emits the
      declared context; dangling entries (no emitting workflow) fail; `pr-gate.yml` may not be registered.
- [ ] 2.3 Assert the aggregation deadline is strictly less than the aggregating job's `timeout-minutes`.

## 3. The aggregator (surface: `scripts/ci/aggregate-required-tiers.sh`)
- [ ] 3.1 Read the registry; poll `GET /repos/{o}/{r}/commits/{head_sha}/check-runs?filter=latest`
      (paginated) for `github.event.pull_request.head.sha`; keep the highest check-run id per context.
- [ ] 3.2 Self-exclude by run identity: job ids from `GET /actions/runs/${GITHUB_RUN_ID}/jobs`, plus a
      details-URL fallback on the same run id. No name matching.
- [ ] 3.3 Fail closed on failed / non-terminal-at-deadline / absent; ignore unregistered contexts;
      back off 15s → 60s; expire into a FAILURE naming every non-terminal tier.
- [ ] 3.4 Waivers: honour `ci:waive:<tier-id>` for absent/pending only, never for a terminal failure;
      no blanket waiver; emit a `::warning::` + summary line per waiver.
- [ ] 3.5 Emit a job summary listing `(context, check-run id, status, conclusion, run URL)` per tier.
- [ ] 3.6 Injectable inputs for offline runs: check-run JSON source, registry path, deadline, poll budget.

## 4. `pr-gate.yml` wiring (surface: `.github/workflows/pr-gate.yml`)
- [ ] 4.1 Split into `pr-gate-core` (today's steps verbatim) and `required` (`name: required` UNCHANGED,
      `needs: [pr-gate-core]`, `if: always()`), so the branch-protection context name is untouched.
- [ ] 4.2 `required` fails whenever `needs.pr-gate-core.result != 'success'`; never skipped; keep the trigger
      unfiltered.
- [ ] 4.3 Add `checks: read` + `actions: read` to `permissions` (today: `contents: read` only) and set the
      aggregating job's `timeout-minutes` above the aggregation deadline.
- [ ] 4.4 Confirm `.github/branch-protection.json` is unchanged (`contexts: ["required"]`).

## 5. Tier conversion (surface: the registered tier workflows, starting with `flight-ci.yml`)
- [ ] 5.1 Replace the trigger `paths:` filter with the always-fire sentinel; add an unconditional classifier
      job emitting the declared context (pattern: `observability-gate.yml` `classify`).
- [ ] 5.2 Classifier computes the mandate from `git diff --name-only base...head`; a mandating diff runs the
      full tier regardless of the `ci:*` label; a non-mandating diff leaves the label as opt-in.
- [ ] 5.3 Inapplicable → emit the context as an explicit success naming the reason; expensive jobs stay
      gated on the classifier output.

## 6. Empirical verification before the registry is populated
- [ ] 6.1 On this change's own PR, assert that check runs from sibling `pull_request` workflows are returned
      for `pull_request.head.sha` (NOT `github.sha`, the merge commit) — record the observed evidence in the
      PR. A wrong key yields a permanently empty set (fails closed, but uselessly).
- [ ] 6.2 Verify the `GITHUB_TOKEN` with `checks: read` can read the endpoint on a fork PR.

## 7. Tests (surface: `scripts/tests/test_aggregate_required_tiers.sh`, `scripts/ci/tests/`)
- [ ] 7.1 Synthetic check-run fixtures: all-pass, one-pending, one-failed, one-absent-and-registered,
      one-absent-and-not-registered, duplicate-context re-run (both directions), self-exclusion.
- [ ] 7.2 A discriminating case per state: assert non-zero exit AND that the offending tier is named.
- [ ] 7.3 Waiver cases: absent+waived passes; failed+waived FAILS; waiver scoped to one tier.
- [ ] 7.4 Non-vacuity: an always-exit-0 stub aggregator turns the suite RED; an always-pass stub enrolment
      rule turns the `validate-workflows.rb` tests RED.
- [ ] 7.5 No wall-clock assertions (#2642) — expiry via an injected already-expired deadline / zero poll
      budget; verify `scripts/tests/check-no-wallclock-asserts.sh` stays clean.
- [ ] 7.6 Wire the suite into `scripts/agent-gate.sh`'s `tooling-tests` component.

## 8. Doctrine
- [ ] 8.1 `CLAUDE.md` autonomy section: `required` aggregates the registered sibling tiers and fails closed
      on failed/pending/absent; arming `--auto` stays correct; tier-then-`required` re-run order.
- [ ] 8.2 `website/src/content/docs/agents-developing/gate-contract.md`: same, plus the registry's location
      and the enrolment rule.
- [ ] 8.3 `docs/ci/ci-tier-policy.md`: record that the sole required context now aggregates, and that new
      `pull_request` workflows must enrol or exempt.
- [ ] 8.4 State explicitly that no step of the worker flow requires applying a tier label.

## 9. Gate / review / audit
- [ ] 9.1 `--lite` green each fix round (summary-file redirect); review-first (rust-reviewer + roborev on the
      lite-green diff) before the one full gate.
- [ ] 9.2 Full `scripts/agent-gate.sh` once pre-merge inside `flow-closer`; C (`spec-auditor`) anchored to
      `openspec/changes/required-aggregates-sibling-tiers/specs/**`; final roborev clean.
