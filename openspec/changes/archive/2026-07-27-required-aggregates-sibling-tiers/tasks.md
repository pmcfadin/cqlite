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
      exactly one job emits the declared context and its condition is EXACTLY `${{ !cancelled() }}` (a bare
      `always()` is rejected: it launders a run cancellation into a `failure` conclusion); for every
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
- [x] 7b.5 P5 the emitting job's condition must be EXACTLY `${{ !cancelled() }}` (round 3: a bare
      `always()` runs the job DURING a cancellation and turns `needs.*.result == cancelled` into a
      `failure`, which makes the supersession grace unreachable).
- [x] 7b.6 P6 structural failing-path check (reads the result AND can exit non-zero, comments and quoted
      strings stripped) replacing the `/exit 1/` substring match.
- [x] 7b.7 P7 the aggregator refuses an empty/unparseable `tiers:` itself.
- [x] 7b.8 P8 a waived ABSENT tier resolves immediately; a waived PENDING one still waits.
- [x] 7b.9 P10 lone check-run object parses; eval'd command inputs shape-validated.
- [x] 7b.10 Mutants for each, including the near-miss inverses that must NOT red.

## 7c. Roborev round 2 (Q1–Q5)
- [x] 7c.1 Q1 the Flight mandate covers everything that reaches Flight at runtime (`cqlite-core/**`,
      `test-data/**`, Cargo manifests, `rust-toolchain.toml`, `setup-rust-ci`), and ONE applicability
      output governs the whole tier — a core-only diff mandates the end-to-end job, not just `--lib`.
      New rules: `applicability_scope_errors`, `mandate_path_errors`.
- [x] 7c.2 Q2 label events queue instead of cancelling (conditional `cancel-in-progress`, unchanged
      concurrency group so two runs can never both report `required`) and skip `pr-gate-core`, whose
      recorded result for the same head sha is required instead (`filter=all` + run-identity exclusion;
      absent/pending/failed/skipped-on-a-non-label-event all fail closed). New rule:
      `aggregator_concurrency_errors`.
- [x] 7c.3 Q3 the aggregator, its modules and the registry are evaluated from the BASE ref (no
      `pull_request_target`; no head content in the job outside the announced bootstrap fallback).
      New rule: `aggregator_trust_boundary_errors`. Residual (a PR still controls the work its own tiers
      do) stated in design.md.
- [x] 7c.4 Q4 the inaccurate "releases the heavy runner" comment corrected; the second-runner cost stated
      in design.md ("Cost"), including the rejected `workflow_run` alternative.
- [x] 7c.5 Q5 the three shebang'd scripts are `100755`; the no-op `sed` expression now deletes the
      paths-ignore sentinel it read as deleting.
- [x] 7c.6 Discrimination mutants for each (policy non-vacuity 10 → 14; aggregator non-vacuity 6 → 8),
      plus real-tree assertions that the shipped configuration — not merely a synthetic one — has the
      properties.

## 7d. Round-3 review (R1-R5): deployment axis, cancellation, portability
- [x] 7d.1 R1 MIGRATION STATE. `scripts/ci/gating_head_emitability.rb` answers "can the tree THIS EVENT
      ran emit this base-registered context at all?" from provable properties only (workflow absent, no
      PR trigger, `types:`/`branches:` excluding this event, no job with that name). A positive answer
      reds on the FIRST poll naming rebase + `ci:waive:<id>`; inconclusive evidence yields nothing; the
      verdict is never a pass. `pr-gate.yml` checks out `github.sha` (the merge commit for a
      pull-request event) sparse/read-only/`continue-on-error`, and the enrolment rule requires BOTH
      halves of that wiring. NO short absent-deadline: a tier gate job `needs:` every other job, so an
      absent context is the normal state of a running tier and a timer would red the PRs that mandate it.
- [x] 7d.2 R2 the tier gate stops laundering a cancellation into a failure: `if: ${{ !cancelled() }}`
      mandated (a bare `always()` rejected by name), `skipped` joins `cancelled`/`stale` as
      non-terminal-then-fail, and `scripts/tests/test_gating_workflow_semantics.sh` asserts the whole
      chain workflow-YAML to conclusion to aggregation verdict, with an `always()` mutant.
- [x] 7d.3 R3 portability: one ruby `subst` fixture editor replaces five newline-in-replacement `sed`
      sites and one GNU relative-range address; a 14-rule GNU-only lint (12 from #2926 plus the two sed
      classes it lacks) covers this change's shell, each rule with a mutant. A repo-wide extension to
      `scripts/ci/**` and `scripts/tests/**` is a refactor (about 85 pre-existing hits) and belongs to
      issue #2981.
- [x] 7d.4 R4 the never-exercised `python3 || ruby` fallbacks are gone; the suites are ruby-only and were
      run with python3 masked off PATH.
- [x] 7d.5 R5 the waiver actor is allowlisted before reaching a `::warning::`; the no-op registry append
      is removed.
- [x] 7d.6 Discrimination mutants for each (policy non-vacuity 14 to 17; aggregator 8 to 10; the
      workflow-semantics chain carries its own `always()` mutant and 14 lint mutants).

## 7e. Round 4 review (3 Medium + 3 Low; five blockers)
- [x] 7e.1 S1 the named security control did not exist: no CODEOWNERS file anywhere, so
      `require_code_owner_reviews` had nothing to resolve. Adds `.github/CODEOWNERS` for `.github/` and
      `scripts/ci/`, and design.md now states the true strength — an automatic review REQUEST, not a
      merge block: live branch protection has `require_code_owner_reviews: false` and zero required
      approvals, and `.github/branch-protection.json` has drifted from it. The residual is recorded as
      VISIBLE-but-uncontrolled; enforcing it is an owner decision. Validated against GitHub's
      `codeowners/errors` endpoint; a self-test asserts coverage, with a rule-removal mutant.
- [x] 7e.2 S2 the tier gate failed OPEN on an unrecognised verdict (`skipped` reads as a pass, which is a
      claim about `run_tier`). The gate now validates the verdict (`true`/`false` only) and additionally
      requires the work to have run when it says the tier applies. Mutants: empty / `maybe` / `TRUE` /
      `1` all red, plus an unvalidated-gate mutant that goes green.
- [x] 7e.3 S3 the break-glass cancelled the tier it waived. The label-churn rule now covers registered
      tiers and demands an ACTION-AWARE `cancel-in-progress` (the round-2 form accepted
      `${{ github.event_name == 'pull_request' }}`, true for label events); flight-ci.yml fixed. Plus the
      aggregator half: a waived tier whose only check run was minted at/after the waiver was applied
      resolves at once, with the before/after pair as the discriminator.
- [x] 7e.4 S4 any check run with the right name satisfied a tier. Provenance (`app` slug/id = GitHub
      Actions + an Actions run `details_url`) is verified fail-closed for tier contexts and for the
      recorded `pr-gate-core` result; an impostor neither satisfies nor SHADOWS the genuine run. Mutants:
      foreign app, no app, foreign URL, and a higher-id forgery over a real failure.
- [x] 7e.5 S5 the ruby floor was load-bearing and unchecked. `scripts/ci/gating_ruby_floor.rb` declares
      ruby >= 3.0 in one place, library callers abort with the remedy, the three self-tests
      SKIP-with-reason, and the dead `ArgumentError` YAML fallback is gone. Mutants: the predicate is
      probed at 2.6/2.7/3.0/3.2/4.0/garbage, and an anti-drift check asserts every gating file requires
      the declaration.
- [x] 7e.6 S6 waiver attribution named `$GITHUB_ACTOR` — the run's actor, not the labeller. Resolved from
      the PR's `labeled` events (last wins), allowlisted at the point of resolution, UNRESOLVED when the
      feed cannot be read. Mutant: the run actor is set to a name that must appear nowhere.
- [x] 7e.7 Campsite: the trigger/concurrency/trust-boundary rules move to
      `scripts/ci/gating_event_rules.rb` (`gating_policy_rules.rb` 769 to 562 lines).
- [x] 7e.8 Discrimination counts after round 4: policy non-vacuity 17 to 19, aggregator 10 to 12; suites
      aggregate 94/0, policy 71/0, semantics 26/0.

## 7f. Round 5 review (1 High + 1 Medium + 2 Low)
- [x] 7f.1 T1 (High) a waiver, once applied, permanently bypassed its tier. `ci:waive:<tier-id>` is a LABEL
      and a label survives a push; read live on every poll AND honoured immediately for an ABSENT tier, it
      excused every later head sha's tier before that tier could mint a check run — so the waiver always
      won the race and "a failed tier cannot be waived" was unenforceable. Round 4's pending shortcut
      inherited it (`started >= waiver_at` is true of ANY later run). The early waiver is now bound to
      EVIDENCE: the `labeled` event must be no older than this head sha's first recorded CI activity (the
      earliest `started_at` over PROVENANCED check runs — a commit timestamp is author-chosen and
      back-datable, so it is not usable), and a pending run must additionally start inside
      `WAIVER_RUN_WINDOW_SECONDS` of that event. Unbound waivers fall back to the deadline rule unchanged,
      so a stale waiver delays a verdict but never pre-empts one. Precedence: impostor > bound waiver >
      migration state > deadline waiver > absent. Mutants: binding-always-true 4 failures, no-window 1,
      anchor-counts-impostors 1.
- [x] 7f.2 T2 migration detection false-redded label-triggered runs — a three-fix interaction (P1 added
      label events to the aggregator, R1 keyed emitability to "this event's activity type", so any label
      change minutes after a push declared a healthy `types: [opened, synchronize]` tier unemittable).
      Emitability is now judged against the activity types that can put the context on THIS HEAD SHA
      (head-producing types plus the current event). That makes `MANDATORY_TIER_PR_TYPES` load-bearing —
      a compliant tier can no longer reach the branch — so the containment of the two constants is
      cross-asserted with a mutant. Mutant (single-event rule restored): 4 failures.
- [x] 7f.3 T3 `Set.new` in `gating_policy_rules.rb` with no `require "set"` — it worked only on
      `gating_registry.rb`'s load order, and `Set` is not autoloaded until ruby 3.1 while the declared
      floor is 3.0. Adds the require plus a static, load-order-independent lint over every gating ruby
      file; the lint immediately found a second instance (`rescue Psych::SyntaxError` with no
      `require "yaml"`). Mutant: deleting the require must be named.
- [x] 7f.4 T4 `provenance` named two unrelated concepts. The waiver side is renamed to
      `parse_waiver_events` / `context[:waiver_events]`, matching the `--waiver-events` flag and
      `WAIVER_EVENTS_CMD` end to end; `provenance_error`/`provenanced?`/`ACTIONS_APP_SLUG` keep the word
      for its one meaning, WHICH APP MINTED A CHECK RUN.
- [x] 7f.5 Owner decision on the 7e.1 drift: **live is correct, the config file was wrong.**
      `.github/branch-protection.json` is reconciled to live (0 approvals, no code-owner reviews, no
      last-push approval) — it is applied verbatim, so an aspirational value would switch `main` to a
      policy merge-on-green cannot satisfy. The rationale is recorded in
      `.github/QUALITY_GATES_ENFORCEMENT.md` and `docs/ci/ci-tier-policy.md`; the verifier in
      `setup-branch-protection.js` (which hardcoded `>= 1` as CRITICAL) derives the expectation from the
      config it applies. design.md now states the residual as **uncontrolled at merge time by design**,
      with CODEOWNERS advisory and `ci:waive:` as the hatch — no blocking control is claimed.
- [x] 7f.6 Discrimination counts after round 5: aggregator non-vacuity 12 to 13; suites aggregate 106/0,
      policy 74/0, semantics 26/0.

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
- [x] 9.2 Full `scripts/agent-gate.sh` once pre-merge inside `flow-closer`; C (`spec-auditor`) anchored to
      `openspec/changes/required-aggregates-sibling-tiers/specs/**`; final roborev clean.
      Gate of record PASS at `9dc735e` (all components, `tree-integrity: PASS`); C PASS (every
      requirement `satisfied` bar one justified `partial`), which caught `design.md` still documenting
      the round-4 waiver rule round 5 had superseded — fixed at `cd4e473` and re-certified `--delta`
      against the `9dc735e` anchor; final roborev clean of blockers (5 findings → #3033, #3034).
